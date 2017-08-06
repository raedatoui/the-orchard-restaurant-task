from datetime import datetime
from google.appengine.ext import ndb
from restaurantfinder.utils import csv_parser


class Restaurant(ndb.Model):
    name = ndb.StringProperty(required=True)
    boro = ndb.StringProperty()
    building = ndb.StringProperty()
    street = ndb.StringProperty()
    zipcode = ndb.StringProperty()
    phone = ndb.StringProperty()
    cuisine = ndb.StringProperty()
    inspection_date = ndb.DateProperty()
    action = ndb.StringProperty()
    violation_code = ndb.StringProperty()
    violation_description = ndb.StringProperty()
    critical_flag = ndb.BooleanProperty()
    score = ndb.IntegerProperty()
    grade = ndb.StringProperty()
    grade_date = ndb.DateProperty()
    record_date = ndb.DateProperty()
    inspection_type = ndb.StringProperty()

    attr_mapping = [
        "CAMIS",
        "DBA",
        "BORO",
        "BUILDING",
        "STREET",
        "ZIPCODE",
        "PHONE",
        "CUISINE DESCRIPTION",
        "INSPECTION DATE",
        "ACTION",
        "VIOLATION CODE",
        "VIOLATION DESCRIPTION",
        "CRITICAL FLAG",
        "SCORE",
        "GRADE",
        "GRADE DATE",
        "RECORD DATE",
        "INSPECTION TYPE"
    ]
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
    def create_from_row(cls, csv_row):
        row = csv_parser.parse_line(csv_row[1])
        row = dict(zip(cls.attr_mapping, row))

        cls(
            name=row["DBA"],
            boro=row["BORO"],
            building=row["BUILDING"],
            street=row["STREET"],
            zipcode=row["ZIPCODE"],
            phone=row["PHONE"],
            cuisine=row["CUISINE DESCRIPTION"],
            inspection_date=cls.get_date(row["INSPECTION DATE"]),
            action=row["ACTION"],
            violation_code=row["VIOLATION CODE"],
            violation_description=row["VIOLATION DESCRIPTION"],
            critical_flag=cls.is_critical(row["CRITICAL FLAG"]),
            score=cls.get_int(row["SCORE"]),
            grade=row["GRADE"],
            grade_date=cls.get_date(row["GRADE DATE"]),
            record_date=cls.get_date(row["RECORD DATE"]),
            inspection_type=row["INSPECTION TYPE"]
        ).put()
