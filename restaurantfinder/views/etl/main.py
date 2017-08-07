import os

from flask import current_app as app
from flask import request, redirect, render_template

from restaurantfinder import etl
from restaurantfinder.blueprints import etl as etl_view
from restaurantfinder.utils import gcs


@etl_view.route('/', methods=['GET'])
def index():
    if etl.Extractor.is_working():
        job = etl.Extractor.get()
        return redirect(job.base_path + "/status?root=" + job.pipeline_id)
    else:
        return render_template("index.html")


@etl_view.route('/', methods=['POST'])
def etl_done():
    job = etl.Extractor.get()
    job.running = False
    job.has_data = True
    job.put()
    return "ok"


@etl_view.route("/start", methods=['GET'])
def etl_start():
    extractor = etl.load_restaurant_data(app.config['RESTAURANTS_CSV'])
    return redirect(
        extractor.base_path + "/status?root=" + extractor.pipeline_id)


@etl_view.route('/local', methods=['GET'])
def etl_start_local():
    filename = "local.csv"
    static = os.path.join(os.path.dirname(__file__), '../..', 'static')
    filepath = os.path.join(static, filename)

    with open(filepath) as f:
        loaded = f.read()
        gcs.write(filename=filename, content_type="text/csv", data=loaded)

    extractor = etl.load_restaurant_data(filename)
    return redirect(
        extractor.base_path + "/status?root=" + extractor.pipeline_id)


@etl_view.route('/cleanup', methods=['GET'])
def cleanup():
    cleaner = etl.clear_restaurant_data()
    return redirect(cleaner.base_path + "/status?root=" + cleaner.pipeline_id)


@etl_view.route('/cleanup', methods=['POST'])
def cleanup_done():
    etl.Extractor.clear()
    return "ok"
