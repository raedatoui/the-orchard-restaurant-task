from restaurantfinder.application import create_app
from restaurantfinder import config

config = config.Config()
app = create_app(config=config)