import logging
import json

import urllib
from google.appengine.api import urlfetch

log = logging.getLogger(__name__)

SERVICE_ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'
MAPS_API_KEY = "AIzaSyCf_98nh6p0zMPbOQ3L7fDfjmPnPWoZDog"


def get_coords_from_address(restaurant):
    address = "{} {} {} {}".format(
        restaurant.building,
        restaurant.street,
        restaurant.boro,
        restaurant.zipcode
    ).replace(" ", "+")

    url = SERVICE_ENDPOINT.format(address, MAPS_API_KEY)
    rpc = urlfetch.create_rpc()
    urlfetch.make_fetch_call(rpc, url)
    return rpc


def get_result(rpc):
    try:
        result = rpc.get_result()
        if result.status_code == 200:
            print result.content
            result = json.loads(result.content)['results'][0]
            return result['formatted_address'], result['geometry']['location']
    except urlfetch.DownloadError:
        log.exception('Caught exception fetching url')
        raise
