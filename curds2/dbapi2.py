#
"""
curds2.dbapi2 module

python DBAPI2-compatible Datascope functionality
"""
import exceptions
import datetime
import logging
try:
    import collections
except ImportError:
    pass
# Check ENV if Antelope is not installed via sitecustomize.py
try:
    from antelope.datascope import (Dbptr, dbtmp, dbALL, dbNULL, dbINVALID,
        dbBOOLEAN, dbDBPTR, dbINTEGER, dbREAL, dbTIME, dbYEARDAY, dbSTRING)
except ImportError:
    import sys
    import os
    sys.path.append(os.path.join(os.environ['ANTELOPE'],'data','python'))
    from antelope.datascope import (Dbptr, dbtmp, dbALL, dbNULL, dbINVALID,
        dbBOOLEAN, dbDBPTR, dbINTEGER, dbREAL, dbTIME, dbYEARDAY, dbSTRING)

LOG = logging.getLogger(__name__)
try:
    LOG.addHandler(logging.NullHandler())
except:
    logging.raiseExceptions = False

# DBAPI top level attributes
apilevel     = "2.0"      # 1.0 or 2.0
threadsafety = 0          # Playing it safe (datascope??)
paramstyle   = "format"   # N/A right now, execute uses Dbptr API

# DBAPI standard exceptions
class Error(exceptions.StandardError):
    pass

class Warning(exceptions.StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

# DBAPI Type Objects / Functions
#----------------------------------------------------------------------------#
class DBAPITypeObject:
    def __init__(self,*values):
        self.values = values
    
    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

STRING   = DBAPITypeObject(dbSTRING)
BINARY   = DBAPITypeObject(None)
NUMBER   = DBAPITypeObject(dbINTEGER, dbREAL, dbBOOLEAN, dbTIME, dbYEARDAY)
DATETIME = DBAPITypeObject(dbTIME, dbYEARDAY)
ROWID    = DBAPITypeObject(dbDBPTR)

Binary    = buffer

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
TimestampFromTicks = Timestamp.fromtimestamp

def DateFromTicks(ticks):
    return Date(TimestampFromTicks(ticks).timetuple()[:3])

def TimeFromTicks(ticks):
    return Time(TimestampFromTicks(ticks).timetuple()[3:6])

#----------------------------------------------------------------------------#

class _Executer(object):
    """
    Executes commands as a function or attribute
    
    Allows method style calls to the Dbptr directly, in addition
    to the standard 'execute' method.

    Notes
    -----
    Calling the methods directly is NOT DBAPI2 standard, but is just
    a convenience that makes sense given the Datascope API

    """
    __slots__ = ['__cursor']

    @staticmethod
    def __execute(cursor, operation, *args, **kwargs):
        """
        Based on original execute function
        """
        if not hasattr(cursor._dbptr, operation):
            raise ProgrammingError("No such command available: " + operation)
        proc = getattr(cursor._dbptr, operation)
        result = proc(*args, **kwargs) 
        
        # Return depends on result
        if isinstance(result, Dbptr):
            if dbINVALID in result:
                raise DatabaseError("Invalid value in pointer: {0}".format(result))
            cursor._dbptr = result
            return cursor.rowcount
        else:
            return result
    
    def __init__(self, cursor):
        """
        Create an object to execute a Dbptr method

        Input
        -----
        DBAPI2 Cursor instance
        
        """
        self.__cursor = cursor

    def __getattr__(self, operation):
        """
        Return a function that calls your method 'operation'
        """
        def _operation(*args, **kwargs):
            return self.__execute(self.__cursor, operation, *args, **kwargs)
        
        return _operation

    def __call__(self, operation, params=[]):
        """
        Standard DBAPI2-style execute as a function
        """
        if isinstance(params, dict):
            result = self.__execute(self.__cursor, operation, **params)
        else:
            result = self.__execute(self.__cursor, operation, *params)
        return result

    
class Row(object):    
    def __new__(cls, cursor, row):
        return tuple(row)


# DBAPI Classes
#----------------------------------------------------------------------------#
class Cursor(object):
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
    #--- Attributes ---------------------------------------------------#
    # PRIVATE
    _dbptr = None             # cursor pointer
    
    # DBAPI
    arraysize = 1             # Step size for fetch
    
    # EXTENSIONS
    connection = None         # Parent Connection
    
    # CUSTOM
    CONVERT_NULL = False      # Convert NULL values to python None
    CONVERT_DATETIME = False  # Convert float datetimes w/ TimestampFromTicks
    row_factory  = Row        # Use this to build rows (default is tuple)

    @property
    def _nullptr(self):
        """
        Return current pointer's NULL record
        """
        null = Dbptr(self._dbptr)
        null.record = dbNULL
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
        if self._dbptr.table == dbALL or dbINVALID in self._dbptr:
            return None
        if 'collections' in globals() and hasattr(collections, 'namedtuple'):
            Tuple = collections.namedtuple('Column', ('name','type_code','display_size','internal_size','precision','scale','null_ok'))
        else:
            Tuple = tuple
        dbptr = Dbptr(self._dbptr)
        table_fields = dbptr.query('dbTABLE_FIELDS')
        description = []
        for dbptr.field, name in enumerate(table_fields):
            if name in table_fields[:dbptr.field]:
                name = '.'.join([dbptr.query('dbFIELD_BASE_TABLE'), name])
            type_code     = dbptr.query('dbFIELD_TYPE')
            display_size  = dbptr.query('dbFORMAT')
            internal_size = dbptr.query('dbFIELD_SIZE')
            precision     = dbptr.query('dbFIELD_FORMAT')
            scale         = None
            null_ok       = name not in dbptr.query('dbPRIMARY_KEY')
            dtup = Tuple(name, type_code, display_size, internal_size, precision, scale, null_ok)
            description.append(dtup)
        return description

    @property
    def rowcount(self):
        if self._dbptr.table >= 0:
            return self._dbptr.query('dbRECORD_COUNT')
        else:
            return -1

    @property
    def rownumber(self):
        return self._dbptr.record
    
    def __init__(self, dbptr, **kwargs):
        """
        Make a Cursor from a Dbptr
        
        Inputs
        ------
        dbptr    : antelope.datascope.Dbptr
        **kwargs : keyword args, where
            -> if a cursor attribute, set attribute value
            -> remaining kwargs passed to dblookup
        
        """
        self._dbptr = Dbptr(dbptr)
        
        # Attributes
        for k in kwargs.keys():
            if hasattr(self, k):
                self.__setattr__(k, kwargs.pop(k))
        
        # Inherit row_factory from Connection if not set on creation
        if self.connection:
            if self.connection.row_factory:
                self.row_factory = self.connection.row_factory
            if self.connection.CONVERT_NULL:
                self.CONVERT_NULL = self.connection.CONVERT_NULL
    
    def __iter__(self):
        """Generator, yields a row from 0 to rowcount"""
        for self._dbptr.record in xrange(self.rowcount):
            yield self._fetch()
    
    @staticmethod
    def _convert_null(value, null):
        if value == null:
            return None
        return value
    
    @staticmethod
    def _convert_dt(value, type_code):
        if type_code == DATETIME and value is not None and isinstance(value, float):
            return TimestampFromTicks(value)
        return value

    def _fetch(self):
        """Pull out a row from DB and increment pointer"""
        desc = self.description
        fields = [d[0] for d in desc]
        row = self._dbptr.getv(*fields)
        if self.CONVERT_NULL:    
            row = [self._convert_null(row[n], null) for n, null in enumerate(self._nullptr.getv(*fields))]
        if self.CONVERT_DATETIME:
            row = [self._convert_dt(row[n], d[1]) for n, d in enumerate(desc)]
        self._dbptr.record += 1
        return self.row_factory(self, row)
    
    def close(self):
        """Close database connection"""
        self._dbptr.close()
    
    @property
    def execute(self):
        """
        Execute Datascope database command

        Because Datascope doesn't have an exposed 'language', and
        most of the functionality is already available through the
        Dbptr API, this is just an attempt at standardizing these calls.
        
        Calls
        =====

        Standard : The standard DBAPI way
        ---------------------------------
        result = cursor.execute(operation, params=[])

            Inputs
            ------
            operation : name of a Dbptr method
            params    : sequence or mapping of parameters for given method
        
        API : Use the Datascope API methods
        -----------------------------------
        result = cursor.execute.operation(*params, **kparams)
            
            Call Dbptr methods directly.

        Returns
        =======
        Depends on command, anything returning a Dbptr modifies the cursor
        and is available through the 'fetch*' methods or by iterating, 
        and returns the number of rows, anything else is returned directly.
        
        Notes
        =====
        The Dbptr API already converts basic types, but not everything, 
        the NULL implementatiion is a mess, and times are just floats, so,
        in the future, could check the type and implement the DBAPI types
        by say, converting any datetime objects to the float stamp expected.
        
        """
        return _Executer(self)

    def executemany(self, operation, param_seq=[]):
        """Execute one command multiple times"""
        for params in param_seq:
            rc = self.execute(operation, params)
        return rc
        
    def fetchone(self):
        """
        Return one row from current pointer.

        Returns
        -------
        tuple or row_factory-generated row

        Notes
        -----
        If the 'dbALL' record is there, just start at first one
        also, rollover to 0 if at the end
        
        """
        if not 0 <= self.rownumber < self.rowcount:
            self._dbptr.record = 0
        return self._fetch()

    def fetchmany(self, size=None):
        """
        Return 'size' number of rows
        
        Inputs
        ------
        size : int of number of records to return (self.arraysize)
        
        Returns
        -------
        list of tuples or row_factory-generated rows

        Notes
        -----
        If no 'size' given, uses the 'arraysize' attribute
        
        If 'size' is more records than are left, functions the same
        as the 'fetchall()' method.
        
        """
        if size is None:
            size = self.arraysize
        end = self.rownumber + size
        if end > self.rowcount:
            end = self.rowcount
        return [self.fetchone() for self._dbptr.record in xrange(self.rownumber, end)]
            
    def fetchall(self):
        """
        Return the rest of the rows

        Returns
        -------
        list of tuples or row_factory-generated rows
        
        """
        return self.fetchmany(size=self.rowcount)
        
    def scroll(self, value, mode='relative'):
        """
        Move the Cursor (rownumber)
        
        Inputs
        ------
        value : int of index movement
        mode  : str of -
            "relative" : move 'value' from current (default)
            "absolute" : move to 'value' record
        
        """
        recnum = self._dbptr.record
        if mode == "relative":
            recnum += value
        elif mode == "absolute":
            recnum = value
        else:
            raise ProgrammingError("Invalid mode: " + mode)
        if 0 <= recnum < self.rowcount:
            self._dbptr.record = recnum
        else:
            raise IndexError("Produces an index out of range: %d of %d rows" % (recnum, self.rowcount))
         

class Connection(object):
    """
    DBAPI compatible Connection type for Datascope
    
    """
    _dbptr = None
    
    cursor_factory = Cursor
    row_factory  = None
    CONVERT_NULL = False

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
            self._dbptr = dbtmp(schema)
        else:
            self._dbptr = Dbptr(database, perm=perm)
        for k in kwargs.keys():
            if hasattr(self, k):
                self.__setattr__(k, kwargs.pop(k))
        
    def close(self):
        self._dbptr.close()

    def __enter__(self):
        """With support"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close Connection if Exception thrown in a 'with'
        """
        if self._dbptr.query('dbDATABASE_COUNT') != 0:
            self.close()
    
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
