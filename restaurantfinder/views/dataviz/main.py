from flask import redirect, render_template

from restaurantfinder import etl
from restaurantfinder.blueprints import dataviz as dataviz_view


@dataviz_view.route('/', methods=['GET'])
def index():
    if etl.Extractor.has_data():
        return render_template("dataviz.html")
    else:
        return redirect("/etl")


