#
"""
Base classes for API
"""

class BaseRow(object):
    def __new__(cls, cursor, row):
        return tuple(row)


class BaseExecuter(object):
    """
    Executes command as a function or attribute
    
    Attributes
    ==========
    cursor : the Cursor calling instance

    Must implement
    ==============
    execute(self, operation, *args)

    """
    __slots__ = ['cursor']
    
    def execute(self, operation, *args):
        pass

    def __init__(self, cursor):
        """
        Create an object to execute a Dbptr method

        Input
        -----
        DBAPI2 Cursor instance
        
        """
        self.cursor = cursor

    def __getattr__(self, operation):
        """
        Return a function that calls your method 'operation'
        """
        def _operation(*args, **kwargs):
            return self.execute(operation, *args, **kwargs)
        
        return _operation

    def __call__(self, operation, params=[]):
        """
        Standard DBAPI2-style execute as a function
        """
        if isinstance(params, dict):
            result = self.execute(operation, **params)
        else:
            result = self.execute(operation, *params)
        return result


class BaseCursor(object):
    """
    Base Cursor class with generic methods

    Must implement
    ==============
    __init__(self, *args, **kwargs) [can use super]
    _fetch(self) [stub]
    description [read only attribute/property]
    rowcount [read only attribute/property]
    """
    _database = None           # cursor pointer
    _table    = None
    _field    = None
    _record   = None
    _executer = BaseExecuter

    # DBAPI
    arraysize = 1           # Step size for fetch
    
    # EXTENSIONS
    connection = None       # Parent Connection
    
    # CUSTOM
    CONVERT_NULL = False    # Convert NULL values to python None
    CONVERT_DATETIME = False
    row_factory  = BaseRow      # Use this to build rows (default is tuple)
    
    @property
    def description(self):
        """Sequence of 7-item sequences"""
        return None

    @property
    def rowcount(self):
        return -1

    def _fetch(self):
        """Fetch row"""
        pass

    def __init__(self, *args, **kwargs):
        """
        Make a new Cursor
        """
        if 'connection' in kwargs:
            self.connection = kwargs.pop('connection')

        # Inherit settings from Connection if exists
        if self.connection:
            if self.connection.row_factory:
                self.row_factory = self.connection.row_factory
            if self.connection.CONVERT_NULL:
                self.CONVERT_NULL = self.connection.CONVERT_NULL
            if self.connection.CONVERT_DATETIME:
                self.CONVERT_DATETIME = self.connection.CONVERT_DATETIME
    
    @property
    def rownumber(self):
        return self._record
        
    def __iter__(self):
        """Generator, yields a row from 0 to rowcount"""
        for self._record in xrange(self.rowcount):
            yield self._fetch()
    
    @staticmethod
    def _convert_null(value, null):
        if value == null:
            return None
        return value
    
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
        This is a hacky way to get at Datascope functions, originally done
        through 'callproc', but this should be the main method, so... 
        
        The Dbptr API already converts basic types, but not everything, 
        the NULL implementatiion is a mess, and times are just floats, so,
        in the future, could check the type and implement the DBAPI types
        by say, converting any datetime objects to the float stamp expected.
        
        """
        return self._executer(self)

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

        If CONVERT_NULL is True, any value equal to its NULL value
        will be a python None.
        
        Notes
        -----
        If the 'dbALL' record is there, just start at first one
        also, rollover to 0 if at the end
        
        """
        if not 0 <= self.rownumber < self.rowcount:
            self._record = 0
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
        return [self.fetchone() for self._record in xrange(self.rownumber, end)]
            
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
        recnum = self._record
        if mode == "relative":
            recnum += value
        elif mode == "absolute":
            recnum = value
        else:
            raise ProgrammingError("Invalid mode: " + mode)
        if 0 <= recnum < self.rowcount:
            self._record = recnum
        else:
            raise IndexError("Produces an index out of range")


class BaseConnection(object):
    """
    Base Connection class with generic methods/constructor
    """
    cursor_factory = None
    row_factory  = BaseRow
    CONVERT_NULL = False
    CONVERT_DATETIME = False
    
    dsn = None

    def __init__(self, dsn, **kwargs):
        """
        Generic connection constructor

        set first arg to self.dsn, rest of kwargs as named attributes
        """
        self.dsn = dsn
        
        for k in kwargs.keys():
            if hasattr(self, k):
                self.__setattr__(k, kwargs.pop(k))
    
    def is_open(self):
        """Return bool if connection(s) open"""
        return True

    def close(self):
        pass
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trackback):
        if self.is_open():
            self.close()
    
    def commit(self):
        pass

    def cursor(self, **kwargs):
        return self.cursor_factory(connection=self, **kwargs)
