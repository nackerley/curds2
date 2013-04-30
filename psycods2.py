#
# dbapi2 module for Datascope
#
import exceptions
try:
    import collections
except ImportError:
    pass
from antelope.datascope import Dbptr, dbALL, dbNULL, dbINVALID

apilevel     = "2.0"      # 1.0 or 2.0
threadsafety = 0          # Playing it safe (datascope??)
paramstyle   = "format"   # use %s style for now

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

class Cursor(object):
    """
    DBAPI 2.0 compatible cursor type for Datascope
    
    Attributes (DBAPI standard)
    ----------
    arraysize   : str of step size for 'fetch'
    description : sequence of 7-item sequence of DBAPI 'description'
    rowcount    : int of number of rows returned by last operation
    rownumber   : int of current record number
    
    Additional attributes
    ---------------------
    CONVERT_NULL : bool of whether to try and change Nulls to None
    row_factory  : function handle to build more complex rows
    
    Methods (DBAPI standard)
    -------
    close() : Close the connection
    callproc(procname, params=[]) : Access to Dbptr functions
    execute(operation, params=[]) : Call dbprocess string command
    executemany(operation, param_seq=[]) : Execute same operation multiple times
    fetchone() : Get record of current pointer
    fetchmany(size=cursor.arraysize) : Get multiple records from current pointer on
    fetchall() : Get all records from current pointer to end
    
    Built-ins
    ---------
    __iter__ : Cursor is a generator which can be iterated over
    
    """
    #--- Attributes ---------------------------------------------------#
    # PRIVATE
    _dbptr = None           # cursor pointer
    
    # DBAPI
    arraysize = 1           # Step size for fetch
    
    # EXTENSIONS
    connection = None       # Not Implemented Yet
    
    # CUSTOM
    CONVERT_NULL = False    # Convert NULL values to python None
    row_factory = None      # Use this to build rows (default is tuple)

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
        
        """
        if self._dbptr.table == dbALL or dbINVALID in self._dbptr:
            return None
        description = []
        dbptr = Dbptr(self._dbptr)
        dbptr.record = dbNULL
        used = []
        for dbptr.field in range(dbptr.query('dbFIELD_COUNT')):
            # get a bunch of stuff, name, type (check for hybid name?)
            name = dbptr.query('dbFIELD_NAME')
            if name in used:
                name = '.'.join([dbptr.query('dbFIELD_BASE_TABLE'), name])
            used.append(name)
            type_code = dbptr.query('dbFIELD_TYPE')
            display_size = dbptr.query('dbFORMAT')
            internal_size = dbptr.query('dbFIELD_SIZE')
            precision = dbptr.query('dbFIELD_FORMAT')
            scale = dbptr.query('dbFIELD_UNITS')
            null_ok = dbptr.getv(name)[0]
            dtup = (name, type_code, display_size, internal_size, precision, scale, null_ok)
            if 'collections' in globals() and hasattr(collections, 'namedtuple'):
                Tuple = collections.namedtuple('Tuple', ('name','type_code','display_size','internal_size','precision','scale','null_ok'))
                dtup = Tuple(*dtup)
            description.append(dtup)
        return description

    @property
    def rowcount(self):
        # try nrecs except return -1
        return self._dbptr.nrecs()
    
    @property
    def rownumber(self):
        return self._dbptr.record
    
    #--- Methods ------------------------------------------------------#
    def __init__(self, dbptr, **kwargs):
        """
        Make a Cursor from a Dbptr
        
        Inputs
        ------
        dbptr    : antelope.datascope.Dbptr
        **kwargs : keyword args passed to dblookup
        
        """
        self._dbptr = Dbptr(dbptr)
        if kwargs:
            self._dbptr = self._dbptr.lookup(**kwargs)
    
    def __iter__(self):
        for self._dbptr.record in xrange(self.rowcount):
            yield self._fetch()
    
    def _none_if_eq(self, v1, v2):
        """Return v1 if v1 != v2, None if v1 == v2"""
        if v1 == v2:
            return None
        else:
            return v1
    
    def _map_ds_nulls(self, row, nullrow):
        """For two seq, replace items in row with None if they == nullrow items"""
        return tuple([self._none_if_eq(r, nullrow[n]) for n, r in enumerate(row)])
    
    def _fetch(self):
        """Pull out a row from DB and increment pointer"""
        fields = [d[0] for d in self.description]
        row = self._dbptr.getv(*fields)
        if self.CONVERT_NULL:
            nullrow = self._nullptr.getv(*fields)
            row = self._map_ds_nulls(row, nullrow)
        if self.row_factory:
            row = self.row_factory(self, row)
        self._dbptr.record += 1
        return row
        
    def close(self):
        self._dbptr.close()

    def callproc(self, procname, params=[]):
        """
        Call stored procedure
        
        Inputs
        ------
        procname : name of a Dbptr method
        params   : sequence of valid parameters for given method
        
        Returns
        -------
        Depends on command, anything returning a Dbptr is treated as an
        'execute', anything else is returned
        
        Notes
        -----
        This is a hacky way to get at Datascope functions, most importantly,
        the 'query', but will work with 'addv', even 'process', etc...
        
        """
        if hasattr(self._dbptr, procname):
            proc = getattr(self._dbptr, procname)
            result = proc(*params)
            if isinstance(result, Dbptr):
                self._dbptr = result
                return self.rowcount
            else:
                return result
        else:
            raise Exception("No such command available: " + procname)
    
    def execute(self, operation, params=[]):
        """
        Execute (send commands to dbprocess)
        
        For API compatibility -  it basically runs like this:
        Dbptr.process( "operation" % (*params) )
        
        """
        # parse the substitutions unsecurely for now
        command_str = operation % tuple(params)
        self._dbptr = self._dbptr.process(command_str)
        return self.rowcount

    def executemany(self, operation, param_seq=[]):
        for params in param_seq:
            rc = self.execute(operation, params)
        return self.rowcount
        
    def fetchone(self):
        """
        Return one row
        
        If the 'dbALL' record is there, just start at first one
        also, rollover to 0 if at the end
        
        """
        if self.rownumber == dbALL or self.rownumber == self.rowcount:
            self._dbptr.record = 0
        return self._fetch()

    def fetchmany(self, size=None):
        """
        Return 'size' number of rows
        
        Inputs
        ------
        size : int of number of records to return (self.arraysize)
        
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
        """Return the rest of the rows"""
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
        if 0 <= value < self.rowcount:
            self._dbptr.record = recnum
        else:
            raise IndexError("Produces an index out of range")
         

class Connection(object):
    """
    DBAPI compatible Connection type for Datascope
    
    """
    _dbptr = None

    row_factory = None

    def __init__(self, database, **kwargs):
        """
        Open a connection to a Datascope database
        
        Inputs
        ------
        database : str name
        perm     : str of permissions
    
        """
        self._dbptr = Dbptr(database, **kwargs)

    def close(self):
        self._dbptr.close()

    #
    # UNRELIABLE count due to some stuff
    #
    #def __del__(self):
    #    if self._dbptr.query('dbDATABASE_COUNT') == 1:
    #        self.close()

    def cursor(self, **kwargs):
        """
        Construct a Cursor object from Connection pointer
        
        Notes
        -----
        (Any kwargs are passed to dblookup)
        
        """
        return Cursor(self._dbptr, **kwargs)
        
def connect(dsn, perm='r'):
    """
    Return a Connection object to a Datascope database
    
    Inputs
    ------
    dsn  : str of name of database (Data Source Name)
    perm : str of permission - passed to Datascope API ('r')
        
    
    """
    return Connection(dsn, perm=perm)

#--- Utilities -------------------------------------------------------#
# Row factory functions, based on sqlite3's DBAPI implementation
# They take a 'row' tuple and the Cursor instance and return a row
# If collections is supported (2.6+ for namedtuple, 2.7+ for OrderedDict)
# Use like this:
# >>> cursor.row_factory = namedtuple_row
#
# the fetch* functions will then return nicer named rows
#
# TODO: Break out all row_factories to a compiled module for speed?
#
def namedtuple_row(cursor, row):
    """
    A row_factory function for nice fast namedtuple rows
    
    EXCEPT IT DOESN'T WORK WITH VIEWS due to Datascope 'dot' syntax
    
    """
    Tuple = collections.namedtuple('Row', [d[0] for d in cursor.description])
    return Tuple(*row)

def ordereddict_row(cursor, row):
    """
    A row_factory function to make OrderedDict rows from row tuple
    
    """
    # Have to build key/value tuple pairs...
    kv = [(d[0], row[n]) for n, d in enumerate(cursor.description)]
    return collections.OrderedDict(kv)
#
#---------------------------------------------------------------------#
