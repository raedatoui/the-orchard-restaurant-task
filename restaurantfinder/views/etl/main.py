import os

from flask import current_app as app
from flask import render_template

from restaurantfinder import etl
from restaurantfinder.blueprints import etl as etl_view
from restaurantfinder.utils import gcs


@etl_view.route('/', methods=['GET'])
def index():
    if etl.Extractor.is_working():
        job = etl.Extractor.get()
        return render_template(
            "monitor.html", nav="etl",
            monitor=job.base_path + "/status?root=" + job.pipeline_id
        )
    else:
        return render_template("index.html", nav="etl")


@etl_view.route("/start", methods=['GET'])
def etl_start():
    filename = app.config['RESTAURANTS_CSV']

    # when working locally, we need to store the CSV file
    # into the local cloud storage.
    if app.config['IS_DEVAPPSERVER']:
        # filename = "local.csv"
        static = os.path.join(os.path.dirname(__file__), '../..', 'static')
        filepath = os.path.join(static, filename)

        with open(filepath) as f:
            loaded = f.read()
            gcs.write(filename=filename, content_type="text/csv", data=loaded)

    extractor = etl.load_restaurant_data(filename)

    return render_template(
        "monitor.html", nav="etl",
        monitor=extractor.base_path + "/status?root=" + extractor.pipeline_id
    )


@etl_view.route('/geocode', methods=['GET'])
def geocode():
    extractor = etl.geocode_restaurant_data()
    return render_template(
        "monitor.html", nav="etl",
        monitor=extractor.base_path + "/status?root=" + extractor.pipeline_id
    )


@etl_view.route('/cleanup', methods=['GET'])
def cleanup():
    cleaner = etl.clear_restaurant_data()
    return render_template(
        "monitor.html", nav="etl",
        monitor=cleaner.base_path + "/status?root=" + cleaner.pipeline_id
    )


@etl_view.route('/cleanup', methods=['POST'])
def cleanup_done():
    etl.Extractor.clear()
    return ""
