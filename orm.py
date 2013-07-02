#
# orm.py -by MCW 2013
#
"""
Base classes and generators for Datascope Object-Relational Mapping 

"""
from antelope.datascope import (Dbptr, dbtmp, dbALL, dbNULL)


class Base(dict, object):
    """
    Holds the pointer to a db record, the db must remain open
    
    Access fields by key, attribute, or get/set methods.
    
    Attributes
    ----------
    TABLE_NAME : name of table, query 'dbTABLE_NAME'
    PRIMARY_KEY : tuple of primary keys, query 'dbPRIMARY_KEY'
    TABLE_FIELDS : tuple of fields, query 'dbTABLE_FIELDS'
    Fields : sorted list of fields

    Methods
    -------
    get(field)        : Same as dict access, but return None if NULL
    set(field, value) : Set to value, set to None if 'value' is NULL

    Notes
    -----
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
        Create a tuple from a database pointer.
        """
        if db:
            if db.record == dbALL:
                raise ValueError("Rec # is 'dbALL', one record only, please.")
            self._ptr = Dbptr(db)
        else:
            self._ptr = Dbptr()
            raise NotImplementedError("No empty contructor allowed here yet...")

        # If instance is tied to a table, make sure it's a pointer to that one
        if hasattr(self, '__tablename__') and self.__tablename__ != self.TABLE_NAME:
            raise ValueError("Not a valid pointer for " +  self.__tablename__)

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
            super(Base,self).__setattr__(field, value)
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
        start = "{0}(".format(self.__class__.__name__)
        # Build up a list containing the fields of the primary key
        # Annoyingly, times have a '::' between them, so deal with that...
        mids = []
        for k in self.PRIMARY_KEY:
            if '::' in k:
                for _k in k.split('::'):
                    mids.append( ( _k, str(self.get(_k)) ) )
            else:
                mids.append( ( k, str(self.get(k)) ) )
        middle = ', '.join(['='.join([_k,_v]) for _k,_v in mids])
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
    
    @property
    def to_dict(self):
        """
        Return a dict of key, value pairs for every field in the record
        
        Acts as a readonly '__dict__' variable

        """
        return dict([(f, self.get(f)) for f in self.TABLE_FIELDS])


class tablemaker(object):
    """
    Generic table class generator

    """
    def __new__(cls, source):
        """
        Create a class from relation source
        
        Input
        -----
        source : dbptr to a table/record OR str of table name

        Returns : Class of type Base, named by table

        """
        if isinstance(source, Dbptr):
            tablename = source.query('dbTABLE_NAME')
        elif isinstance(source, str):
            tablename = source
        else:
            raise ValueError('Input a Dbptr or string to create a class')
        tabletype = type.__new__(type, tablename.capitalize(), (Base,), {'__tablename__': tablename})
        
        return tabletype


class RowProxy(object):
    """
    Make a 'row' record instance of a table class from a Dbptr
    
    Input
    -----
    dbptr : Dbptr to a record

    Returns : instance of a Base class created by tablemaker

    """
    def __new__(cls, dbptr):
        Table = tablemaker(dbptr)
        return Table(dbptr)


