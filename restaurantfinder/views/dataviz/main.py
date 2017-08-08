from flask import redirect, render_template, request

from restaurantfinder import etl
from restaurantfinder.blueprints import dataviz as dataviz_view


@dataviz_view.route('/', methods=['GET'])
def visualize():
   # if etl.Extractor.has_data():
    return render_template("dataviz.html", nav="dataviz")
    #else:
     #   return redirect("/etl")