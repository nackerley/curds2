#
"""
curds2.dbapi2 module

python DBAPI2-compatible Datascope functionality

This module uses instances of the antelope.datascope.Dbptr
(All Dbptr instance methods are available through execute)

See curds2.raw.dbapi2 for a fully functional implementation of the
"raw" functions. This is essentially the "raw" version with Dbptr on top.
So any commands sent through 'execute' should be Dbptr methods, rather than
'_datascope' functions.

"""
from curds2.api.core import *
from curds2.raw.dbapi2 import (
    ds, Connection as RawConnection, Cursor as RawCursor, BaseExecuter,
    STRING, BINARY, NUMBER, DATETIME, ROWID)
from antelope.datascope import Dbptr


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
    @staticmethod
    def _raw(db):
        return [db.database, db.table, db.field, db.record]

    def execute(self, operation, *args, **kwargs):
        """
        Based on original execute function
        """
        dbptr = Dbptr(self.cursor._dbptr)
        if not hasattr(dbptr, operation):
            raise ProgrammingError("No such command available: " + operation)
        proc = getattr(dbptr, operation)
        result = proc(*args, **kwargs) 
        
        # Return depends on result
        if isinstance(result, Dbptr):
            ptr = self._raw(result)
            if ds.dbINVALID in ptr:
                raise DatabaseError("Invalid value in pointer: {0}".format(ptr))
            self.cursor._dbptr = ptr
            return self.cursor.rowcount
        else:
            return result


class Cursor(RawCursor):
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
    CONVERT_DATETIME : bool of whether to convert timestamps to datetimes
    row_factory  : function handle to build more complex rows
    
    Methods (DBAPI standard)
    -------
    close() : Close the connection
    execute(operation, params=[]) : Call Dbptr method
    executemany(operation, param_seq=[]) : Execute operation multiple times
    fetchone() : Get record of current pointer
    fetchmany(size=cursor.arraysize) : Get multiple records
    fetchall() : Get all records from current pointer to end
    
    Extension methods
    -----------------
    scroll(record, mode="relative") : Move cursor pointer to a record

    Built-ins
    ---------
    __iter__ : Cursor is a generator which can be iterated over
    
    """
    _executer = _Executer


class Connection(RawConnection):
    """
    DBAPI compatible Connection type for Datascope
    """
    cursor_factory = Cursor


def connect(dsn=':memory:', perm='r', **kwargs):
    """
    Return a Connection object to a Datascope database
    
    Inputs
    ------
    dsn  : str of name of database (':memory:')
    perm : str of permission - passed to Datascope API ('r')
        
    """
    return Connection(dsn, perm=perm, **kwargs)
