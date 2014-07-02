#
"""
curds2.cursors
"""
from curds2.dbapi2 import Cursor, ds


class RowPointerDict(dict):
    """Quickie class to map db fields to dict keys""" 
    _dbptr = None
    _tbl = None
    _keys = None

    def __init__(self, db=None, keys=[]):
        self._dbptr = db
        self._tbl = ds._dbquery(self._dbptr, ds.dbTABLE_NAME)
        self._keys = keys

    def __getitem__(self, key):
        out = ds._dbgetv(self._dbptr, self._tbl, key)
        # Non-backwards compatibility strikes again.
        if len(out) > 1:
            return out[1][0]
        else:
            return out[0]

    def __setitem__(self, key, value):
        ds._dbputv(self._dbptr, self._tbl, key, value)

    def __len__(self):
        return ds._dbquery(self._dbptr, ds.dbRECORD_COUNT)

    def update(self, dict_):
        args = []
        for i in dict_.items():
            args.extend(i)
        ds._dbputv(self._dbptr, self._tbl, *args)

    def keys(self):
        return self._keys

    def values(self):
        return [self[k] for k in self.keys()]

    def items(self):
        return [(k, self[k]) for k in self.keys()]


class InteractiveCursor(Cursor):
    """
    Cursor class that returns non-standard interactive rows which point
    to rows in the database and contain no data
    """
    def _fetch(self):
        k = [d[0] for d in self.description]
        row = RowPointerDict(self._dbptr, keys=k)
        self._record += 1
        return row
