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

    # take advantage of the mapper step and filter out restaurants
    # based on desired criteria:
    # 1- cuisine is Thai
    # 2- no less than a B or a score of 27 or higher
    # 3- no graded restaurants will be skipped

    # we filter the restaurant data right away instead
    # creating a large database of all restaurants
    # we do this because we want to use the geocoding service which is heavily
    # rate limited in free mode.

    if str(row[models.CUISINE_FIELD]).lower() == "thai":
        try:
            score = int(row["SCORE"])
            if score <= 27:
                models.Restaurant.create_from_row(row)
        except Exception:
            log.info("skipped restaurant without a score")


def geocode_restaurant(entity):
    geo_rpc = geocoding_service.get_coords_from_address(entity)
    address, lat_lng = geocoding_service.get_result(geo_rpc)
    entity.address = address
    entity.lat = float(lat_lng['lat'])
    entity.lng = float(lat_lng['lng'])
    entity.put()
    return


def delete_restaurant(entity):
    return entity.key.delete()


class ResetPipeline(base_handler.PipelineBase):
    def run(self):
        yield mapreduce_pipeline.MapperPipeline(
            "reset",
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
            "extract",
            "restaurantfinder.etl.create_restaurant",
            "restaurantfinder.utils.gcs_reader.GoogleStorageLineInputReader",
            params={
                "input_reader": {
                    "file_paths": [file_path]
                }
            },
            shards=50
        )


class GeocoderPipeline(base_handler.PipelineBase):
    def run(self):
        yield mapreduce_pipeline.MapPipeline(
            "geocoder",
            "restaurantfinder.etl.geocode_restaurant",
            "mapreduce.input_readers.DatastoreInputReader",
            params={
                "entity_kind": "restaurantfinder.models.Restaurant"
            },
            shards=2
        )


class Extractor(ndb.Model):
    pipeline_id = ndb.StringProperty()
    base_path = ndb.StringProperty()

    @classmethod
    def get(cls):
        r = cls.query().fetch()
        if len(r) == 0:
            return None
        job = r[0]
        return mapreduce_pipeline.MapreducePipeline.from_id(job.pipeline_id)

    @classmethod
    def set(cls, pipeline):
        log.info("updating current job{}".format(pipeline.pipeline_id))
        r = cls.query().fetch()
        if len(r) == 0:
            job = cls()
        else:
            job = r[0]
        job.base_path = pipeline.base_path
        job.pipeline_id = pipeline.pipeline_id
        job.put()

    @classmethod
    def clear(cls):
        r = cls.query().fetch()
        if len(r) == 0:
            return None
        job = r[0]
        job.key.delete()

    @classmethod
    def is_working(cls):
        pipeline = cls.get()
        if not pipeline:
            return False
        return not pipeline.has_finalized

    @classmethod
    def has_data(cls):
        pipeline = cls.get()
        if not pipeline:
            return False
        return pipeline.class_path == "restaurantfinder.etl.ExtractPipeline" \
               and pipeline.has_finalized

    @classmethod
    def has_geo(cls):
        pipeline = cls.get()
        if not pipeline:
            return False
        return pipeline.class_path == "restaurantfinder.etl.GeocoderPipeline" \
               and pipeline.has_finalized


def geocode_restaurant_data():
    job = GeocoderPipeline()
    job.start()
    Extractor.set(job)
    return job


def load_restaurant_data(filename):
    job = ExtractPipeline(filename)
    job.start()
    Extractor.set(job)
    return job


def clear_restaurant_data():
    job = ResetPipeline()
    job.start()
    Extractor.set(job)
    return job
