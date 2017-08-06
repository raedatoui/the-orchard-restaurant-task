import os
from flask import render_template, redirect
from flask import current_app as app
import pipeline
import mapreduce
from google.appengine.ext import db
from restaurantfinder.blueprints import etl as etl_view
from restaurantfinder import etl
from pipeline.models import _PipelineRecord, _SlotRecord, _BarrierRecord, _StatusRecord, _BarrierIndex
from restaurantfinder.utils import gcs
import logging


@etl_view.route('/', methods=['GET'])
def index():
    extractor = etl.load_restaurant_data()
    return redirect(extractor.base_path + "/status?root=" + extractor.pipeline_id)
    #render_template("index.html")


@etl_view.route('/cleanup', methods=['GET'])
def cleanup():
    cleaner = etl.clear_restaurant_data()
    return redirect(cleaner.base_path + "/status?root=" + cleaner .pipeline_id)


@etl_view.route('/sample', methods=['GET'])
def sample():
    filename = app.config['RESTAURANTS_CSV']
    static = os.path.join(os.path.dirname(__file__), '../..', 'static')
    filepath = os.path.join(static, filename)

    with open(filepath) as f:
         loaded = f.read()
         gcs.write(filename=filename, content_type="text/csv", data=loaded)

    return redirect("/mapreduce/pipeline/status")
