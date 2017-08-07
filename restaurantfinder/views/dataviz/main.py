from flask import redirect, render_template

from restaurantfinder import etl
from restaurantfinder.blueprints import dataviz as dataviz_view
from restaurantfinder import models

@dataviz_view.route('/', methods=['GET'])
def index():
    if etl.Extractor.has_data():
        restaurants = models.Restaurant.get_best_rated()
        return render_template("dataviz.html",
                               nav="dataviz",
                               restaurants=restaurants)
    else:
        return redirect("/etl")


@dataviz_view.route('/visualize', methods=['GET'])
def visualize():
    if etl.Extractor.has_data():
        return render_template("dataviz.html")
    else:
        return redirect("/etl")