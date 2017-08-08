import logging
from datetime import datetime
from datetime import timedelta
from itertools import groupby

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
    def recent_grade_fn(self):
        """
        Set the most recent letter grade
        :return:
        """
        grades = self.get_grades()
        return grades[0][1].pop().grade if len(grades) > 0 else ''

    def recent_critical_count_fn(self):
        """
        This aggregates all the recent critical flags since the most recent inspection.
        It includes the flags that were part of the latest grade, and any new inspections since even if they
        didnt produce a grade yet.
        :return: int
        """
        grades = self.get_grades()
        if len(grades) == 0:
            return 0
        latest_inspection = grades[0][0]
        return len([x.critical_flag for x in self.inspections if x.inspection_date >= latest_inspection and x.critical_flag])

    def recent_average_score_fn(self):
        """
        This aggregates all the recent scores since the most recent inspection.
        It includes the score that were part of the latest grade, and any new inspections since even if they
        didn't produce a grade yet.
        :return: int
        """
        grades = self.get_grades()
        if len(grades) == 0:
            return 0
        latest_inspection = grades[0][0]
        inspections = [x for x in self.inspections if x.score > 0 and x.inspection_date >= latest_inspection]
        if len(inspections) == 0:
            return 0
        return float(sum([x.score for x in inspections])) / float(len(inspections))

    def average_score_fn(self):
        """
        Averages the score for all inspections with a score higher than 0
        Inspections with no score generally dont relate to food handling but rather the general facility
        :return:
        """
        if len(self.inspections_keys) == 0:
            return 0
        inspections = [x for x in self.inspections if x.score > 0]
        if len(inspections) == 0:
            return 0
        return float(sum([x.score for x in inspections])) / float(len(inspections))

    def get_grades(self):
        grades = [x for x in self.inspections if x.grade in ['A', 'B', 'C']]
        grouped = groupby(grades, lambda x: x.grade_date)
        result = []
        for date, group in grouped:
            result.append((date, list(group)))
        return result

    def total_grades_fn(self):
        return len(self.get_grades())

    def total_a_fn(self):
        return len([x for x in self.get_grades() if x[1].pop().grade == 'A'])

    def total_critical_fn(self):
        return len([x for x in self.inspections if x.critical_flag])

    def ratio_fn(self):
        total = self.total_grades
        return float(self.total_a) / float(total) if total > 0 else 0

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

    recent_grade = ndb.ComputedProperty(recent_grade_fn)
    recent_grade_date = ndb.DateProperty()
    recent_critical_count = ndb.ComputedProperty(recent_critical_count_fn)
    recent_average_score = ndb.ComputedProperty(recent_average_score_fn)
    average_score = ndb.ComputedProperty(average_score_fn)
    total_a = ndb.ComputedProperty(total_a_fn)
    total_grades = ndb.ComputedProperty(total_grades_fn)
    ratio = ndb.ComputedProperty(ratio_fn)
    total_critical = ndb.ComputedProperty(total_critical_fn)

    def _pre_put_hook(self):
        """
        Set the most recent letter grade date.
        Cant use computed property, as they only support primitive data types.
        :return:
        """
        grades = self.get_grades()
        self.recent_grade_date = grades[0][1].pop().grade_date if len(grades) > 0 else None

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
    def get_page(cls, page):
        qry = cls.query()
        page_size = 20
        restaurants, _, more = qry.fetch_page(
            page_size=page_size,
            offset=(page - 1) * page_size
        )
        pager = {
            'page_size': page_size,
            'page': page,
            'has_next': more,
            'has_prev': page > 1,
            'next_page': page + 1 if more else page,
            'prev_page': max(page - 1, 1),
            'path': ''
        }
        return restaurants, pager

    @classmethod
    def get_geo(cls):
        return cls.query().fetch(projection=[
            cls.name, cls.lat, cls.lng
        ])

    @classmethod
    def find_by_criteria(cls, criteria, page):
        qry = cls.query()
        page_size = 10

        if criteria == 'best':
            msg = "Best guess: most recent grade is A, and has lowest critical flags since"
            qry = qry.filter(cls.recent_grade == 'A').order(cls.recent_critical_count, cls.recent_average_score)

        elif criteria == 'best2':
            msg = "Best guess: most recent grade is A, and has lowest critical flags since, all within the last year (newest restaurants)"
            d = datetime.today() - timedelta(days=365)
            qry = qry.filter(cls.recent_grade == 'A', cls.recent_grade_date >= d).order(-cls.recent_grade_date, cls.recent_critical_count, cls.recent_average_score)

        elif criteria == 'score':
            msg = "Lowest Average Score"
            qry = qry.filter(cls.total_grades > 1).order(cls.total_grades, cls.average_score)

        elif criteria == 'grade':
            msg = "Total Number of As"
            qry = qry.order(-cls.total_a)

        elif criteria == 'critical':
            msg = "Total Number of As with least Critical Flags"
            qry = qry.filter(cls.total_a > 0).order(-cls.total_a).order(
                cls.total_critical)

        elif criteria == 'scorecount':
            msg = "Total Number of As, Lowest Average Score"
            qry = qry.order(-cls.total_a, cls.average_score)

        elif criteria == 'ratio':
            msg = "Ratio of As / Total grades, sorted by Total # of As"
            qry = qry.order(-cls.ratio, -cls.total_a)

        elif criteria == 'recent':
            msg = "Latest Grade is A, Lowest Average Score"
            qry = qry. \
                filter(cls.recent_grade == 'A', cls.average_score > 0.0) \
                .order(cls.average_score)
        else:
            raise Exception("incorrect criteria")

        restaurants, _, more = qry.fetch_page(
            page_size=page_size,
            offset=(page - 1) * page_size
        )
        pager = {
            'page_size': page_size,
            'page': page,
            'has_next': more,
            'has_prev': page > 1,
            'next_page': page + 1 if more else page,
            'prev_page': max(page - 1, 1),
            'path': 'top/{}'.format(criteria)
        }
        return restaurants, msg, pager


def create_from_row(row):
    """
    This function create the Restaurant and its Inspections.
    If the restaurant doesnt exist, then we call the geocoding
    service to get some nicely formatted Geo data.
    :param row: csv row
    :return: Restaurant entity
    """
    restaurant = Restaurant.get_by_id(row["CAMIS"], use_cache=True)
    needs_geo = False
    if restaurant is None:
        restaurant = Restaurant(
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
        inspection_date=Restaurant.get_date(row["INSPECTION DATE"]),
        # action=row["ACTION"],
        # violation_code=row["VIOLATION CODE"],
        violation_description=row["VIOLATION DESCRIPTION"],
        critical_flag=Restaurant.is_critical(row["CRITICAL FLAG"]),
        score=Restaurant.get_int(row["SCORE"]),
        grade=row["GRADE"],
        grade_date=Restaurant.get_date(row["GRADE DATE"]),
        inspection_type=row["INSPECTION TYPE"]
    ).put_async()

    # if we need geo,run the service and update the entity
    if needs_geo:
        geo_rpc = geocoding_service.get_coords_from_address(restaurant)
        address, lat, lng = geocoding_service.get_result(geo_rpc)
        restaurant.address = address
        restaurant.lat = lat
        restaurant.lng = lng

    inspection = ndb_rpc.get_result()
    restaurant.inspections_keys.append(inspection)
    restaurant.put()

