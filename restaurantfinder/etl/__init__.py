import csv
import os
from flask import current_app as app
import logging
from restaurantfinder import application

log = logging.getLogger(__name__)


def load_restaurant_data():
    filename = os.path.join(application.APP_ROOT_FOLDER, app.config['RESTAURANTS_CSV'])
    log.info(filename)
    with open(filename, "rb") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            log.info(row)
            yield row

