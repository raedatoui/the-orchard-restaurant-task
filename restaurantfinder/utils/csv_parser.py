import csv
import StringIO

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


class MyDialect(csv.Dialect):
    strict = True
    skipinitialspace = True
    quoting = csv.QUOTE_ALL
    delimiter = ','
    quotechar = '"'
    lineterminator = '\n'


def parse_line(line):
    b = StringIO.StringIO(line)
    r = csv.reader(b, MyDialect())
    for row in r:
        return dict(zip(attr_mapping, row))
