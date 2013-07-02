
from antelope.datascope import (Dbptr, dbopen, dblookup, dbALL, dbNULL)

def _open(database, perm='r', **kwargs):
    """
    Return a pointer to an open database from a string or Dbptr.
    Any keyword arguments not for dbopen are passed to dblookup
    """
    if isinstance(database, Dbptr):
        db = Dbptr(database)
    elif isinstance(database, str):
        db = dbopen(database, perm=perm)
    else:
        raise TypeError("Input pointer or string of valid database")
    return db

class Dbtuple(dict, object):
    """
    Holds the pointer to a db record, NOT the data, can access the
    same as Dbrecord, but the pointer must remain open

    Useful for large datasets that may have trouble in memory
    Only stores the pointer, not contents, all attributes are
    returned by querying the open db using the pointer.
    """
    # Only holds one thing in Python namespace, Dbptr object:
    _ptr = Dbptr()
    
    # built in queries for useful info
    @property
    def TABLE_NAME(self):
        return self._ptr.query('dbTABLE_NAME')  # string of what table record came from
    
    @property
    def PRIMARY_KEY(self):
        return self._ptr.query('dbPRIMARY_KEY') # tuple of strings of fields in primary key
    
    @property
    def TABLE_FIELDS(self):              # tuple of fields from database record
        return self._ptr.query('dbTABLE_FIELDS')
    
    @property
    def Fields(self):                   # May go away in future
        flist = list(self.TABLE_FIELDS)
        flist.sort()
        return flist
    
    @property
    def _ptrNULL(self):
        """
        Return NULL record for a given pointer
        """
        nullptr = Dbptr(self._ptr)
        nullptr.record = dbNULL
        return nullptr

    def __init__(self, db=None):
        """
        Testing object relational mapper-type thing...
        """
        if db:
            if db.record == dbALL:
                raise ValueError("Rec # is 'dbALL', one record only, please.")
            self._ptr = Dbptr(db)
        else:
            self._ptr = Dbptr()
            raise NotImplementedError("No empty contructor allowed here yet...")

    def __getattr__(self, field):
        """
        Looks for attributes in fields of a db pointer
        """
        return self._ptr.getv(field)[0]

    def __setattr__(self, field, value):
        """Try to set a db field

        You must have opened your db with r+ permissions!
        """
        # Special case: trying to set the pointer. Else try to write to the db
        if field == '_ptr':
            super(Dbtuple,self).__setattr__(field, value)
        else:
            # Could try to catch an ElogComplain in else, but the same
            # error comes up for read-only or a wrong field
            if self._ptr.query('dbDATABASE_IS_WRITABLE'):
                self._ptr.putv(field, value)
            else:
                raise IOError("Database not opened with write permission!")

    # Dictionary powers activate:
    __getitem__ = __getattr__
    __setitem__ = __setattr__

    def _null(self, field):
        """
        Returns NULL value for a given field
        """
        return self._ptrNULL.getv(field)[0]

    def get(self, field):
        """Get a database value from the given field (NULL supported)
        
        If the value is a NULL value for that field, return a python None
        """
        value = self.__getattr__(field)
        if value == self._null(field):
            value = None
        return value

    def set(self, field, value):
        """Set a database field to the given value (NULL supported)
        
        Setting a field to 'None' puts a NULL value in for that record field
        """
        if value is None:
            value = self._null(field)
        self.__setattr__(field, value)

    def __repr__(self):
        """
        Useful representation - shows the table and primary key of the record.
        """
        start = "{0}('{1}' -> ".format(self.__class__.__name__, self.TABLE_NAME)
        # Build up a list containing the fields of the primary key
        # Annoyingly, times have a '::' between them, so deal with that...
        mids = []
        for k in self.PRIMARY_KEY:
            if '::' in k:
                keyf = '::'.join([str(self.__getattr__(_k)) for _k in k.split('::')])
            else:
                keyf = str(self.__getattr__(k))
            mids.append(keyf)
        middle = ' '.join(mids)
        end = ")"
        return start+middle+end

    def __str__(self):
        """
        Prints out record content as a string.

        SHOULD be the same as if you cat'ted a line from the table text file
        """
        db = Dbptr(self._ptr)
        formatting = ' '.join([db.query('dbFIELD_FORMAT') for db.field in range(len(self.TABLE_FIELDS))])
        fields = tuple([self.__getattr__(f) for f in self.TABLE_FIELDS])
        return formatting % fields


class Base(object):
    """
    Creates a database row class instance using Dbtuple

    Based on the SQLAlchemy naming scheme
    
    Notes
    -----
    Could make into Dbtuple metaclass, difference might just be semantics

    """
    @staticmethod 
    def _repr(self):
        """
        Rep string for new Base classes

        self : any instance of Dbtuple

        """
        start = "{0}(".format(self.__class__.__name__)
        # Build up a list containing the fields of the primary key
        # Annoyingly, times have a '::' between them, so deal with that...
        mids = {}
        for k in self.PRIMARY_KEY:
            if '::' in k:
                for _k in k.split('::'):
                    mids[_k] = str(self.__getattr__(_k))
            else:
                mids[k] = str(self.__getattr__(k))
        middle = ' '.join(['='.join([_k,_v]) for _k,_v in mids.iteritems()])
        end = ")"
        return start+middle+end
    
    def __new__(cls, dbptr):
        """
        Create a class from relation source and return an instance

        Each object is an inherited instance of a Dbtuple

        """
        table = dbptr.query('dbTABLE_NAME')
        tabletype = type.__new__(type, table.capitalize(), (Dbtuple,), {'__tablename__': table, '__repr__' : cls._repr})
        return tabletype(dbptr)

class _BaseRow(object):
    """
    Row for DBAPI2 using ORM class.

    TEST - should work but not efficient implementation...
    (override Cursor would be best??)

    """
    def __new__(cls, cursor, row):
        return Base(cursor._dbptr)


class Dialect(object):
    """Stub Dialect"""
    pass


class Datascope_Psycods2(Dialect):
    """
    Dialect for Datascope using the DBAPI2.0
    """
    
    name = 'datascope'
    driver = 'psycods2'
    
    #def __init__(self):
    #    self.drv = __import__(self.driver)
        
    def create_connect_args(self, url):
        #row = getattr(self.drv, 'NamedTupleRow')
        row = _BaseRow
        return {'row_factory' : row, 'CONVERT_NULL' : True}


