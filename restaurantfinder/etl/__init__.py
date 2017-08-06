from __future__ import absolute_import

import logging

import cloudstorage.cloudstorage_api as gcs
from flask import current_app as app
from google.appengine.api import memcache
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline

from restaurantfinder import models
from restaurantfinder.utils import gcs

log = logging.getLogger(__name__)


def create_restaurant(row):
    # do process with csv row
    models.Restaurant.create_from_row(row)
    return


def delete_restaurant(entity):
    yield entity.key.delete()


class ResetPipeline(base_handler.PipelineBase):
    def run(self):
        yield mapreduce_pipeline.MapperPipeline(
            "Reset Datastore",
            handler_spec="restaurantfinder.etl.delete_restaurant",
            input_reader_spec="mapreduce.input_readers.DatastoreInputReader",
            params={
                "entity_kind": "restaurantfinder.models.Restaurant"
            },
            shards=50
        )


class ExtractPipeline(base_handler.PipelineBase):
    def run(self, filename):
        file_path = gcs.abs_filename(filename)
        yield mapreduce_pipeline.MapperPipeline(
            "Populate Datastore",
            "restaurantfinder.etl.create_restaurant",
            "restaurantfinder.utils.gcs_reader.GoogleStorageLineInputReader",
            params={
                "input_reader": {
                    "file_paths": [file_path]
                }
            },
            shards=50
        )


def load_restaurant_data(filename):
    with app.app_context():
        extractor = ExtractPipeline(filename)
        set_current_extractor(extractor)
        extractor.start()
        return extractor


def clear_restaurant_data():
    cleaner = ResetPipeline()
    cleaner.start()
    return cleaner


def set_current_extractor(pipeline):
    memcache.add('current_extractor', pipeline.pipeline_id)


def clear_current_extractor():
    memcache.delete('current_extractor')


def is_working():
    with app.app_context():
        print memcache.get('current_extractor')
        return memcache.get('current_extractor') is not None