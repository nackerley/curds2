#
"""
curds2.c_dbapi2 module for Datascope

Uses the base python wrappers
"""
from copy import copy
try:
    import collections
except ImportError:
    pass

from curds2.api.core import *
from curds2.api.base import BaseConnection, BaseCursor, BaseExecuter

# Antelope/Datascope
#----------------------------------------------------------------------------#
try:
    from antelope import _datascope as ds
except ImportError:
    import sys
    import os
    sys.path.append(os.path.join(os.environ['ANTELOPE'],'data','python'))
    from antelope import _datascope as ds

STRING   = DBAPITypeObject(ds.dbSTRING)
BINARY   = DBAPITypeObject(None)
NUMBER   = DBAPITypeObject(ds.dbINTEGER, ds.dbREAL, ds.dbBOOLEAN, ds.dbTIME, ds.dbYEARDAY)
DATETIME = DBAPITypeObject(ds.dbTIME, ds.dbYEARDAY)
ROWID    = DBAPITypeObject(ds.dbDBPTR)


# Utility classes
#----------------------------------------------------------------------------#
def _dbptr(value):
    """Map cursor to dbptr"""
    if hasattr(value, '_dbptr'):
        return value._dbptr
    return value


class _Executer(BaseExecuter):
    """
    Executes commands as a function or attribute
    
    Allows method style calls to the Dbptr directly, in addition
    to the standard 'execute' method.

    Notes
    -----
    Calling the methods directly is NOT DBAPI2 standard, but is just
    a convenience that makes sense given the Datascope API

    """
    def execute(self, operation, *args):
        """
        Based on original execute function
        """
        fxn = '_' + operation
        args = [_dbptr(a) for a in args]
        # Call if exists
        if not hasattr(ds, fxn):
            raise ProgrammingError("No such command available: " + fxn)
        proc = getattr(ds, fxn)
        result = proc(self.cursor._dbptr, *args) 
        
        # Return depends on result
        if isinstance(result, list) and len(result) == 4:
            if ds.dbINVALID in result:
                raise DatabaseError("Invalid value in pointer: {0}".format(result))
            self.cursor._dbptr = result
            return self.cursor.rowcount
        else:
            return result

# DBAPI Classes
#----------------------------------------------------------------------------#
class Cursor(BaseCursor):
    """
    DBAPI 2.0 compatible cursor type for Datascope
    
    Attributes (DBAPI standard)
    ----------
    arraysize   : str of step size for 'fetch'
    description : list of 7-item sequence of DBAPI 'description'
    rowcount    : int of number of rows returned by last operation
    rownumber   : int of current record number
    connection  : instance of Connection 'parent'

    Additional attributes
    ---------------------
    CONVERT_NULL : bool of whether to try and change Nulls to None
    row_factory  : function handle to build more complex rows
    
    Methods (DBAPI standard)
    -------
    close() : Close the connection
    execute(operation, params=[]) : Call Dbptr method
    executemany(operation, param_seq=[]) : Execute same operation multiple times
    fetchone() : Get record of current pointer
    fetchmany(size=cursor.arraysize) : Get multiple records from current pointer on
    fetchall() : Get all records from current pointer to end
    
    Extension methods
    -----------------
    scroll(record, mode="relative") : Move cursor pointer to a record

    Built-ins
    ---------
    __iter__ : Cursor is a generator which can be iterated over
    
    """
    _executer = _Executer

    @property
    def _nullptr(self):
        """
        Return current pointer's NULL record
        
        """
        null = copy(self._dbptr)
        null[3] = ds.dbNULL
        return null

    @property
    def description(self):
        """
        Return readonly 'description' sequence per DBAPI specs
        
        sequence of 7-item sequence of:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        
        Notes
        -----
        Will return a namedtuple if available

        """
        if self._table == ds.dbALL or ds.dbINVALID in self._dbptr:
            return None
        if 'collections' in globals() and hasattr(collections, 'namedtuple'):
            Tuple = collections.namedtuple('Column', ('name','type_code','display_size','internal_size','precision','scale','null_ok'))
        else:
            Tuple = tuple
        dbptr = self._nullptr
        table_fields = ds._dbquery(dbptr, 'dbTABLE_FIELDS')
        description = []
        for dbptr[2], name in enumerate(table_fields):
            if name in table_fields[:dbptr[2]]:
                name = '.'.join([ds._dbquery(dbptr, ds.dbFIELD_BASE_TABLE), name])
            type_code     = ds._dbquery(dbptr, ds.dbFIELD_TYPE)
            display_size  = ds._dbquery(dbptr, ds.dbFORMAT)
            internal_size = ds._dbquery(dbptr, ds.dbFIELD_SIZE)
            precision     = ds._dbquery(dbptr, ds.dbFIELD_FORMAT)
            scale         = None
            null_ok       = name not in ds._dbquery(dbptr, ds.dbPRIMARY_KEY)
            
            dtup = Tuple(name, type_code, display_size, internal_size, precision, scale, null_ok)
            description.append(dtup)
        return description

    @property
    def rowcount(self):
        if self._table >= 0:
            return ds._dbquery(self._dbptr, ds.dbRECORD_COUNT)
        else:
            return -1

    def __init__(self, dbptr, **kwargs):
        """
        Make a Cursor from a Dbptr
        
        Inputs
        ------
        dbptr    : antelope.datascope.Dbptr
        **kwargs : keyword args, where
            -> if a cursor attribute, set attribute value
        
        """
        super(Cursor, self).__init__(**kwargs)

        self._dbptr = copy(dbptr)
        
        # Attributes
        for k, v in kwargs.items():
            if hasattr(self, k):
                self.__setattr__(k, v)

    def __iter__(self):
        """Generator, yields a row from 0 to rowcount"""
        for self._record in xrange(self.rowcount):
            yield self._fetch()
    
    @staticmethod
    def _convert_dt(value, type_code):
        if type_code == DATETIME and value is not None and isinstance(value, float):
            return TimestampFromTicks(value)
        return value

    def _fetch(self):
        """Pull out a row from DB and increment pointer"""
        tbl = ds._dbquery(self._dbptr, ds.dbTABLE_NAME)  # TODO: check view compat
        desc = self.description
        fields = [d[0] for d in desc]
        row = ds._dbgetv(self._dbptr, tbl, *fields)
        if self.CONVERT_NULL:    
            row = [self._convert_null(row[n], null) for n, null in enumerate(ds._dbgetv(self._nullptr, tbl, *fields))]
        if self.CONVERT_DATETIME:
            row = [self._convert_dt(row[n], d[1]) for n, d in enumerate(desc)]
        self._record += 1
        return self.row_factory(self, row)

    def close(self):
        """Close database connection"""
        ds._dbclose(self._dbptr)
         

class Connection(BaseConnection):
    """
    DBAPI compatible Connection type for Datascope
    
    """
    _dbptr = None
    cursor_factory = Cursor

    def __init__(self, database, perm='r', schema='css3.0', **kwargs):
        """
        Open a connection to a Datascope database

        Pass python None to open a db in memory.
        
        Inputs
        ------
        database : str name (':memory:')
        perm     : str of permissions
        schema   : str of temp schema
    
        """
        if database == ":memory:":
            self._dbptr = ds._dbtmp(schema)
        else:
            self._dbptr = ds._dbopen(database, perm)
        for k in kwargs.keys():
            if hasattr(self, k):
                self.__setattr__(k, kwargs.pop(k))
        
    def close(self):
        ds._dbclose(self._dbptr)

    def is_open(self):
        return ds._dbquery(self._dbptr, ds.dbDATABASE_COUNT) != 0
    
    def cursor(self, **kwargs):
        """
        Construct a Cursor object from Connection pointer
        
        Notes
        -----
        (Any kwargs are passed to dblookup)
        
        """
        return self.cursor_factory(self._dbptr, connection=self, **kwargs)
    

def connect(dsn=':memory:', perm='r', **kwargs):
    """
    Return a Connection object to a Datascope database
    
    Inputs
    ------
    dsn  : str of name of database (':memory:')
    perm : str of permission - passed to Datascope API ('r')
        
    """
    return Connection(dsn, perm=perm, **kwargs)
