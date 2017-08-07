from __future__ import absolute_import

import logging

import cloudstorage.cloudstorage_api as gcs
from flask import current_app as app
from google.appengine.ext import ndb
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline

from restaurantfinder import models
from restaurantfinder.utils import csv_parser, gcs, geocoding_service

log = logging.getLogger(__name__)


def create_restaurant(csv_row):
    # do process with csv row
    row = csv_parser.parse_line(csv_row[1])
    model_rpc = models.Restaurant.create_from_row(row)
    geo_rpc = geocoding_service.get_coords_from_address(row)
    try:
        result = model_rpc.get_result()
        address, lat_lng = geocoding_service.get_result(geo_rpc)
        m = result.get()
        m.address = address
        m.lat = float(lat_lng['lat'])
        m.lng = float(lat_lng['lng'])
        m.put()
    except Exception:
        raise
    return

def delete_restaurant(entity):
    return entity.key.delete()


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
        yield mapreduce_pipeline.MapPipeline(
            "Populate Datastore",
            "restaurantfinder.etl.create_restaurant",
            "restaurantfinder.utils.gcs_reader.GoogleStorageLineInputReader",
            params={
                "input_reader": {
                    "file_paths": [file_path]
                },
                "done_callback": "/etl/",
                "done_callback_method": "POST"
            },
            shards=50
        )


class Extractor(ndb.Model):
    pipeline_id = ndb.StringProperty()
    base_path = ndb.StringProperty()
    running = ndb.BooleanProperty()
    has_data = ndb.BooleanProperty()

    @classmethod
    def get(cls):
        r = cls.query().fetch()
        if len(r) == 0:
            return None
        return r[0]

    @classmethod
    def set(cls, pipeline):
        log.info("updating current job{}".format(pipeline.pipeline_id))
        job = cls.get()
        if not job:
            job = cls()
        job.running = True
        job.has_data = False
        job.base_path = pipeline.base_path
        job.pipeline_id = pipeline.pipeline_id
        job.put()

    @classmethod
    def clear(cls):
        job = cls.get()
        job.running = False
        job.put()

    @classmethod
    def is_working(cls):
        job = cls.get()
        if not job:
            return False
        return job.running

    @classmethod
    def has_data(cls):
        job = cls.get()
        if not job:
            return False
        return job.has_data


def load_restaurant_data(filename):
    with app.app_context():
        job = ExtractPipeline(filename)
        job.start()
        Extractor.set(job)
        return job


def clear_restaurant_data():
    job = ResetPipeline()
    job.start()
    Extractor.set(job)
    return job
