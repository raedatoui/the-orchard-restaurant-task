import csv
import StringIO


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
        return row
