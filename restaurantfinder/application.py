import os

from importlib import import_module

from flask import Flask
import pipeline
import mapreduce

import restaurantfinder as app_root
from restaurantfinder.blueprints import all_blueprints

APP_ROOT_FOLDER = os.path.abspath(os.path.dirname(app_root.__file__))
TEMPLATE_FOLDER = os.path.join(APP_ROOT_FOLDER, 'templates')
STATIC_FOLDER = os.path.join(APP_ROOT_FOLDER, 'static')


def create_app(config):
    """Flask application factory. Initializes and returns the Flask application.

    Blueprints are registered in here.

    Modeled after: http://flask.pocoo.org/docs/patterns/appfactories/

    Positional arguments:
    config -- configuration object to load into app.config.

    Returns:
    The initialized Flask application.
    """

    # Initialize app. Flatten config_obj to dictionary (resolve properties).
    app = Flask(__name__)
    config_dict = dict(
        [(k, getattr(config, k)) for k in dir(config) if
         not k.startswith('_')])

    app.config.update(config_dict)

    for bp in all_blueprints:
        import_module(bp.import_name)
        app.register_blueprint(bp)

    pipeline.set_enforce_auth(False)

    # Return the application instance.
    return app
