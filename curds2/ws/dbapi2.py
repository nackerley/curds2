#
"""
curds2.dbapi2 module for Datascope

Uses the base python wrappers
"""
import urlparse
import urllib2

from curds2.api.core import *

# Shim in types for now
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



def connect(dsn=None, perm='r', **kwargs):
    """
    Return a Connection object to a Datascope database
    """
    url = urlparse.urlparse(dsn)
    return Connection(url, **kwargs)



