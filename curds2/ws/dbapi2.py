#
"""
curds2.dbapi2 module for Datascope

Uses the base python wrappers
"""
#import urlparse
import urllib2
import json

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
    """
    Stub Cursor class for a remote client
    """
    _request = {'jsonrpc': '2.0'}
    _headers = {'content-type': 'application/json'}
    _rows = []
    
    description = []
    
    def __init__(self, *args, **kwargs):
        """Constructor"""
        super(Cursor, self).__init__(**kwargs)
        
        self.dsn = self.connection.dsn
        
        # Attributes
        for k, v in kwargs.items():
            if hasattr(self, k):
                self.__setattr__(k, v)

    @staticmethod
    def _convert_dt(value, type_code):
        if type_code == DATETIME and value is not None and isinstance(value, float):
            return TimestampFromTicks(value)
        return value
    
    def _fetch(self):
        n = self._record
        desc = self.description
        row = self._rows[n]
        self._record += 1
        if self.CONVERT_DATETIME:
            row = [self._convert_dt(row[n], d[1]) for n, d in enumerate(desc)]
        return self.row_factory(self, row)

    @property
    def rowcount(self):
        return len(self._rows)
    
    def execute(self, operation, params=[]):
        """
        Call server at a URL and get JSONRPC `result
        """
        curs_settings = ('CONVERT_NULL',)  # to forward to server Cursor
        curs_params = dict([(p, getattr(self, p)) for p in curs_settings])
        rpc_params = {'args': params, 'cursor': curs_params}
        self._request.update({'method': operation, 'params': rpc_params, 'id': 1})
        req = urllib2.Request(self.dsn, json.dumps(self._request), self._headers)
        rep = urllib2.urlopen(req)
        j = rep.read()
        rep.close()
        reply = json.loads(j)
        if reply.get('error'):
            e = reply['error']
            raise DatabaseError(': '.join([e['type'], e['message']]))
        result = reply.get('result')
        if isinstance(result, dict) and 'cursor' in result:
            _curs = result['cursor']
            self.description = _curs.get('description')
            self._rows = _curs.get('rows')
            return self.rowcount
        else:
            return result

class Connection(BaseConnection):
    """
    Connection class for remote
    
    Stub to hold data for Cursor.execute
    """
    cursor_factory = Cursor


def connect(dsn, *args, **kwargs):
    return Connection(dsn, *args, **kwargs)
