#
"""
curds2.cursors
"""
from curds2.dbapi2 import Cursor, ds
from curds2.raw.dbapi2 import _select, _query


class RowPointerDict(dict):
    """
    Row class to map db fields to dict keys
    """
    __slots__ = ['_dbptr', '_tbl', '_keys']

    def __init__(self, db=None, keys=[]):
        self._dbptr = db
        self._tbl = _query(self._dbptr, ds.dbTABLE_NAME)
        self._keys = keys
    
    def __contains__(self, k):
        """
        D.__contains__(k) -> True if D has a key k, else False
        """
        if k in self.keys():
            return True
        else:
            return False

    def __getitem__(self, key):
        return _select(self._dbptr, self._tbl, key)[0]

    def __setitem__(self, key, value):
        ds._dbputv(self._dbptr, self._tbl, key, value)

    def __len__(self):
        return _query(self._dbptr, ds.dbRECORD_COUNT)

    def update(self, dict_):
        args = []
        for i in dict_.items():
            if self.__contains__(i[0]):
                args.extend(i)
        ds._dbputv(self._dbptr, self._tbl, *args)

    def keys(self):
        return self._keys

    def values(self):
        return [self[k] for k in self.keys()]

    def items(self):
        return [(k, self[k]) for k in self.keys()]

    def get(self, k, d=None):
        if k in self:
            return self[k]
        else:
            return d


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

    def append(self, row):
        n = self.execute('addnull', [])
        n = self.scroll(n, 'absolute')
        newrow = self.fetchone()
        newrow.update(row)
        
