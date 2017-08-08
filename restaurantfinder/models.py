from datetime import datetime
import logging
import urlparse
from google.appengine.ext import ndb
from restaurantfinder.utils import geocoding_service
log = logging.getLogger(__name__)

CUISINE_FIELD = "CUISINE DESCRIPTION"


class Inspection(ndb.Model):
    inspection_date = ndb.DateProperty()
    # action = ndb.StringProperty()
    # violation_code = ndb.StringProperty()
    violation_description = ndb.StringProperty()
    critical_flag = ndb.BooleanProperty()
    score = ndb.IntegerProperty()
    grade = ndb.StringProperty()
    grade_date = ndb.DateProperty()
    inspection_type = ndb.StringProperty()


class Restaurant(ndb.Model):
    name = ndb.StringProperty(required=True)

    # geocoding properties
    address = ndb.StringProperty()
    lat = ndb.FloatProperty()
    lng = ndb.FloatProperty()

    building = ndb.StringProperty()
    street = ndb.StringProperty()
    boro = ndb.StringProperty()
    zipcode = ndb.StringProperty()
    phone = ndb.StringProperty()
    cuisine = ndb.StringProperty()
    inspections_keys = ndb.KeyProperty(repeated=True)

    def to_dict(self, include=None, exclude=None):
        result = super(Restaurant, self).to_dict(include=include,
                                                 exclude=exclude)
        for d in ['inspection_date', 'grade_date']:
            v = getattr(self, d)
            if v is not None:
                result[d] = v.strftime('%m/%d/%y')
            else:
                result[d] = None

        return result

    @property
    def inspections(self):
        return sorted(
            [key.get() for key in self.inspections_keys],
            key=lambda x: x.inspection_date, reverse=True)

    @staticmethod
    def is_critical(v):
        if v == "Critical":
            return True
        elif v == "Not Critical":
            return False
        return None

    @staticmethod
    def get_date(v):
        if v == '':
            return None
        try:
            return datetime.strptime(v, '%m/%d/%Y')
        except Exception:
            try:
                return datetime.strptime(v, '%m/%d/%y')
            except Exception:
                return None

    @staticmethod
    def get_int(v):
        try:
            return int(v)
        except Exception:
            return 0

    @classmethod
    def create_from_row(cls, row):
        """
        This function create the Restaurant and its Inspections.
        If the restaurant doesnt exist, then we call the geocoding
        service to get some nicely formatted Geo data.
        :param row: csv row
        :return: Restaurant entity
        """
        restaurant = cls.get_by_id(row["CAMIS"])
        needs_geo = False
        if restaurant is None:
            restaurant = cls(
                id=row["CAMIS"],
                name=row["DBA"],
                phone=row["PHONE"],
                cuisine=row["CUISINE DESCRIPTION"],
                building=row["BUILDING"],
                street=row["STREET"],
                boro=row["BORO"],
                zipcode=row["ZIPCODE"],
            )
            needs_geo = True

        # async datastore call
        ndb_rpc = Inspection(
            inspection_date=cls.get_date(row["INSPECTION DATE"]),
            # action=row["ACTION"],
            # violation_code=row["VIOLATION CODE"],
            violation_description=row["VIOLATION DESCRIPTION"],
            critical_flag=cls.is_critical(row["CRITICAL FLAG"]),
            score=cls.get_int(row["SCORE"]),
            grade=row["GRADE"],
            grade_date=cls.get_date(row["GRADE DATE"]),
            inspection_type=row["INSPECTION TYPE"]
        ).put_async()

        # if we need geo,run the service and update the entity
        if needs_geo:
            geo_rpc = geocoding_service.get_coords_from_address(restaurant)
            address, lat_lng = geocoding_service.get_result(geo_rpc)
            restaurant.address = address
            restaurant.lat = float(lat_lng['lat'])
            restaurant.lng = float(lat_lng['lng'])

        inspection = ndb_rpc.get_result()
        restaurant.inspections_keys.append(inspection)
        restaurant.put()

    @classmethod
    def get_page(cls, page):
        query = cls.query()
        restaurants, _, more = query.fetch_page(page_size=20,
                                              offset=(page - 1) * 20)
        pager = {
            'has_next': more,
            'has_prev': page > 1,
            'next_page': page + 1 if more else page,
            'prev_page': max(page - 1, 1)
        }
        return restaurants, pager

    @classmethod
    def get_best_rated(cls):
        #qry = cls.query()
        # instances, _, more = qry.fetch_page(page_size=20,
        #                                       offset=(page - 1) * limit)
        return cls.query().fetch(20, projection=[
            cls.name, cls.address
        ])

    @classmethod
    def get_geo(cls):
        return cls.query().fetch(projection=[
            cls.name, cls.lat, cls.lng
        ])