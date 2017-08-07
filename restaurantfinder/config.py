import os


class Config(object):
    """Default Flask configuration"""
    DEBUG = True
    TESTING = False
    RESTAURANTS_CSV = "DOHMH_New_York_City_Restaurant_Inspection_Results.csv"
    MAPS_API_KEY = "AIzaSyCf_98nh6p0zMPbOQ3L7fDfjmPnPWoZDog"

    @property
    def IS_DEVAPPSERVER(self):
        return os.environ.get('SERVER_SOFTWARE', '').startswith('Development')

