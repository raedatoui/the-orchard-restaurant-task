from __future__ import absolute_import

import logging

import cloudstorage.cloudstorage_api as gcs
from flask import current_app as app

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from restaurantfinder.utils import gcs
from google.appengine.ext import ndb

from restaurantfinder import models

log = logging.getLogger(__name__)

STORAGE_LOCAL_BUCKET_PREFIX = '/_ah/gcs'
STORAGE_LIFE_BUCKET_PREFIX = 'https://storage.googleapis.com'

FILE_PATH = None


def create_restaurant(row):
    # do process with csv row
    models.Restaurant.create_from_row(row)
    return


def delete_restaurant(entity):
    yield entity.key.delete()


class ResetPipeline(base_handler.PipelineBase):
    def run(self):
        yield mapreduce_pipeline.MapperPipeline(
            "delete_restaurant",
            handler_spec="restaurantfinder.etl.delete_restaurant",
            input_reader_spec="mapreduce.input_readers.DatastoreInputReader",
            params={
                "entity_kind": "restaurantfinder.models.Restaurant"
            }
        )


class ExtractPipeline(base_handler.PipelineBase):
    def run(self):
        global FILE_PATH

        yield mapreduce_pipeline.MapperPipeline(
            "gcs_csv_reader_job",
            "restaurantfinder.etl.create_restaurant",
            "restaurantfinder.utils.gcs_reader.GoogleStorageLineInputReader",
            params={
                "input_reader": {
                    "file_paths": [FILE_PATH]
                }
            })


def load_restaurant_data():
    global FILE_PATH
    with app.app_context():
        filename = app.config['RESTAURANTS_CSV']
        file_path = gcs.abs_filename(filename)
        FILE_PATH = file_path
        extractor = ExtractPipeline()
        extractor.start()
        return extractor


def clear_restaurant_data():
    global FILE_PATH
    with app.app_context():
        cleaner = ResetPipeline()
        cleaner.start()
        return cleaner
