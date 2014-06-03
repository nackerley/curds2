#
"""
curds2.dbapi2 module for Datascope

Uses the base python wrappers
"""
import urlparse
import urllib2

from curds2.api.core import *
from curds2.api.base import *

# Shim in hardcoded Datascope types for now
dbBOOLEAN = 1
dbINTEGER = 2
dbREAL = 3
dbTIME = 4
dbYEARDAY = 5
dbSTRING = 6
dbDBPTR = 142

STRING   = DBAPITypeObject(dbSTRING)
BINARY   = DBAPITypeObject(None)
NUMBER   = DBAPITypeObject(dbINTEGER, dbREAL, dbBOOLEAN, dbTIME, dbYEARDAY)
DATETIME = DBAPITypeObject(dbTIME, dbYEARDAY)
ROWID    = DBAPITypeObject(dbDBPTR)

# implement Connection, Cursor, etc
class Cursor(BaseCursor):
    pass


class Connection(BaseConnection):
    pass


def connect(dsn, *args, **kwargs):
    pass
