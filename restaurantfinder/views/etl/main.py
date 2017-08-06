import os

from flask import current_app as app
from flask import request, redirect, render_template

from restaurantfinder import etl
from restaurantfinder.blueprints import etl as etl_view
from restaurantfinder.utils import gcs


@etl_view.route('/', methods=['GET'])
def index():
    print etl.is_working()
    if etl.is_working():
        return redirect("/mapreduce/status")
    else:
        return render_template("index.html")


@etl_view.route('/cleanup', methods=['GET'])
def cleanup():
    cleaner = etl.clear_restaurant_data()
    return redirect(cleaner.base_path + "/status?root=" + cleaner.pipeline_id)


@etl_view.route('/cleanup/done', methods=['POST'])
def cleanup_done():
    print request.headers
    print request.form
    return redirect("/etl")


@etl_view.route("/start", methods=['GET'])
def start_etl():
    extractor = etl.load_restaurant_data(app.config['RESTAURANTS_CSV'])
    return redirect(
        extractor.base_path + "/status?root=" + extractor.pipeline_id)


@etl_view.route('/local', methods=['GET'])
def start_local_etl():
    filename = "small.csv"
    static = os.path.join(os.path.dirname(__file__), '../..', 'static')
    filepath = os.path.join(static, filename)

    with open(filepath) as f:
        loaded = f.read()
        gcs.write(filename=filename, content_type="text/csv", data=loaded)

    extractor = etl.load_restaurant_data(filename)
    return redirect(
        extractor.base_path + "/status?root=" + extractor.pipeline_id)


@etl_view.route('/done', methods=['POST'])
def etl_done():
    print request.headers
    print request.form
    etl.clear_current_extractor()
    return redirect("/etl")
