#
# orm.py -by MCW 2013
#
"""
Base classes and generators for Datascope Object-Relational Mapping 

Classes
-------
Base : ORM base for a record, methods to access database fields

tablemaker : Make a table class which inherits Base

Schema : A class whose instances have attributes containing
          table classes for every table in a given schema.

RowProxy : create an instance of table class from a Dbptr

"""
from antelope.datascope import (Dbptr, dbtmp, dbALL, dbNULL)

class Base(dict, object):
    """
    Holds the pointer to a db record, the db must remain open
    
    Access fields by key, attribute, or get/set methods.
    
    Attributes
    ----------
    __tablename__ : name of table, query 'dbTABLE_NAME'
    PRIMARY_KEY   : tuple of primary keys, query 'dbPRIMARY_KEY'
    TABLE_FIELDS  : tuple of fields, query 'dbTABLE_FIELDS'
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
    # ATTRIBUTES
    _ptr = Dbptr()
    
    __tablename__ = None

    PRIMARY_KEY  = None
    TABLE_FIELDS = None

    ### MAY BE DEPRICATED IN FUTURE !!! ######################################
    #--- built-in self queries for useful info ------------------------------#
    @property
    def _TABLE_NAME(self):
        return self.get_tablename(self._ptr)
    
    @property
    def _PRIMARY_KEY(self):
        return self.get_primarykey(self._ptr)
    
    @property
    def _TABLE_FIELDS(self):
        return self.get_tablefields(self._ptr)
    
    @property
    def Fields(self):
        flist = list(self._TABLE_FIELDS)
        flist.sort()
        return flist
    #------------------------------------------------------------------------#
    
    @property
    def _ptrNULL(self):
        """
        Return NULL record for a given pointer
        """
        nullptr = Dbptr(self._ptr)
        nullptr.record = dbNULL
        return nullptr
    
    @staticmethod
    def get_tablename(dbptr):
        return dbptr.query('dbTABLE_NAME')

    @staticmethod
    def get_primarykey(dbptr):
        return dbptr.query('dbPRIMARY_KEY')
    
    @staticmethod
    def get_tablefields(dbptr):
        return dbptr.query('dbTABLE_FIELDS')
    
    def __init__(self, db=None):
        """
        Create a tuple instance from a database pointer.
    
        """
        if db:
            if db.record == dbALL:
                raise ValueError("Rec # is 'dbALL', one record only, please.")
            self._ptr = Dbptr(db)
        else:
            self._ptr = Dbptr()
            raise NotImplementedError("No empty contructor allowed here yet...")
        
        # If instance is tied to a table, make sure it's a pointer to that one
        if hasattr(self, '__tablename__') and self.__tablename__ != self._TABLE_NAME:
            raise ValueError("Not a valid pointer for " +  self.__tablename__)
        
        if self.PRIMARY_KEY is None:
            self.PRIMARY_KEY  = self._PRIMARY_KEY
        if self.TABLE_FIELDS is None:
            self.TABLE_FIELDS = self._TABLE_FIELDS

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
    
    def to_dict(self):
        """
        Return a dict of key, value pairs for every field in the record
        
        Acts as a readonly '__dict__' variable

        """
        return dict([(f, self.get(f)) for f in self.TABLE_FIELDS])

# Metaclass type to make table classes on the fly
class tablemaker(type):
    """
    Generic table class generator

    """
    def __new__(cls, source):
        """
        Create a class from relation source
        
        Input
        -----
        source : dbptr to a table/record OR str of table name

        Returns : Class of type tablemaker, inherits Base, named by table

        """
        if isinstance(source, Dbptr):
            dict_ = { '__tablename__' : Base.get_tablename(source),
                       'PRIMARY_KEY'  : Base.get_primarykey(source),
                       'TABLE_FIELDS' : Base.get_tablefields(source),
                    }
        elif isinstance(source, str):
            dict_ = { '__tablename__' : source }
        else:
            raise ValueError('Input a Dbptr or string to create a class')
        tabletype = super(tablemaker, cls).__new__(cls, dict_['__tablename__'].capitalize(), (Base,), dict_ )
        
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


class Schema(object):
    """
    Namespace containing all classes for a given schema
    
    Attributes
    ----------
    Uppercase names of all tables in a schema
    -> containing a class for each one

    """
    def __init__(self, schema='css3.0'):
        """
        Make a schema of classes for all tables

        >>> s = Schema(schema='css3.0')
        >>> s.Assoc
        orm.Assoc
        
        """
        db = dbtmp(schema) 
        for t in db.query('dbSCHEMA_TABLES'):
            _db = db.lookup(table=t)
            self.__setattr__(t.capitalize(), tablemaker(_db))
        db.close()



