from __future__ import absolute_import

import csv
import logging
import os

import cloudstorage as gcss
import cloudstorage.cloudstorage_api as gcs
from flask import current_app as app
from google.appengine.api import app_identity

log = logging.getLogger(__name__)

STORAGE_LOCAL_BUCKET_PREFIX = '/_ah/gcs'
STORAGE_LIFE_BUCKET_PREFIX = 'https://storage.googleapis.com'

FILE_PATH = None


class DatastoreError(Exception):
    pass


def abs_filename(filename):
    bucket_name = app_identity.get_default_gcs_bucket_name()

    if bucket_name is None:
        raise DatastoreError(
            "Error", "Default GCS bucket is not set for this environment"
        )

    return '/{0}/{1}'.format(bucket_name, filename)


def read(filename):
    """
    includes the bucket. comes from a stored render
    :param filename:
    :return:
    """
    file_path = abs_filename(filename)
    gcs_file = gcs.open(file_path)
    content = gcs_file.read()
    gcs_file.close()
    return content


from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from restaurantfinder.etl import gcs_reader


def testMapperFunc(row):
    # do process with csv row
    log.info(row)
    return


class TestGCSReaderPipeline(base_handler.PipelineBase):
    def run(self):
        global FILE_PATH
        yield mapreduce_pipeline.MapPipeline(
            "gcs_csv_reader_job",
            "restaurantfinder.etl.testMapperFunc",
            "restaurantfinder.etl.gcs_reader.GoogleStorageLineInputReader",
            params={
                "input_reader": {
                    "file_paths": [FILE_PATH]
                }

            })



def load_restaurant_data():
    global FILE_PATH
    with app.app_context():
        filename = app.config['RESTAURANTS_CSV']
        file_path = abs_filename(filename)
        FILE_PATH = file_path
        upload_task = TestGCSReaderPipeline()
        upload_task.start()
    # filename = app.config['RESTAURANTS_CSV']
    # file_path = abs_filename(filename)
    # log.info("starting to extract CSV {}".format(file_path))
    # with gcs.open(file_path, 'r') as csv_file:
    #     csv_file.seek(1024, os.SEEK_CUR)
    #     delimiter = ','
    #     reader = csv.reader(csv_file, delimiter=delimiter)
    #
    #     write_retry_params = gcss.RetryParams(backoff_factor=1.1)
    #     current_out_path = abs_filename(
    #          'stream.csv'
    #     )
    #     current_gcs_file = gcs.open(current_out_path, 'w', content_type='text/csv',
    #                         retry_params=write_retry_params)
    #     current_out_writer = csv.writer(current_gcs_file, delimiter=delimiter)
    #     for i, row in enumerate(reader):
    #         current_out_writer.writerow(row)
    #     current_gcs_file.close()


def split(filehandler, delimiter=',', row_limit=10000,
          output_name_template='output_%s.csv', output_path='.',
          keep_headers=True):
    """
    Splits a CSV file into multiple pieces.

    A quick bastardization of the Python CSV library.
    Arguments:
        `row_limit`: The number of rows you want in each output file. 10,000 by default.
        `output_name_template`: A %s-style template for the numbered output files.
        `output_path`: Where to stick the output files.
        `keep_headers`: Whether or not to print the headers in each output file.
    Example usage:

        >> from toolbox import csv_splitter;
        >> csv_splitter.split(open('/home/ben/input.csv', 'r'));

    """
    import csv
    reader = csv.reader(filehandler, delimiter=delimiter)

    current_piece = 1
    current_limit = row_limit
    write_retry_params = gcss.RetryParams(backoff_factor=1.1)
    current_out_path = abs_filename(
        output_name_template % current_piece
    )
    current_gcs_file = gcs.open(current_out_path, 'w', content_type='text/csv',
                                retry_params=write_retry_params)
    current_out_writer = csv.writer(current_gcs_file, delimiter=delimiter)

    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_gcs_file.close()
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = abs_filename(
                output_name_template % current_piece
            )
            current_gcs_file = gcs.open(current_out_path, 'w',
                                        content_type='text/csv',
                                        retry_params=write_retry_params)
            current_out_writer = csv.writer(current_gcs_file,
                                            delimiter=delimiter)

        current_out_writer.writerow(row)
