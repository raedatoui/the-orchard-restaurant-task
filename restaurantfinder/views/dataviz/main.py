from flask import redirect, render_template, request, jsonify

from restaurantfinder import etl
from restaurantfinder.blueprints import dataviz as dataviz_view
from restaurantfinder import models


@dataviz_view.route('/', methods=['GET'])
def visualize():
    if etl.Extractor.has_data():
        return render_template("dataviz.html", nav="dataviz")
    else:
        return redirect("/etl")


@dataviz_view.route('/top/<criteria>.json', methods=['GET'])
def top(criteria):
    if etl.Extractor.has_data():
        try:
            page = int(request.args.get('page'))
        except Exception:
            page = 1

        restaurants, msg, _ = models.Restaurant.find_by_criteria(criteria, page)
        result = [x.to_geo() for x in restaurants]
        return jsonify({
            'restaurants': result,
            'msg': msg
        })

    else:
        return redirect("/etl")
