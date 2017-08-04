from flask import render_template

from restaurantfinder.blueprints import etl as etl_view
from restaurantfinder import etl


@etl_view.route('/', methods=['GET'])
def index():
    etl.load_restaurant_data()
    return render_template("index.html")
