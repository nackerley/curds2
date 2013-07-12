
from antelope.datascope import Dbptr, dbALL, dbNULL


class _RowProxy(object):

    _dbptr = None
    
    __tablename__ = None

    PRIMARY_KEY  = None
    TABLE_FIELDS = None

    ### MAY BE DEPRICATED IN FUTURE !!! ######################################
    #--- built-in self queries for useful info ------------------------------#
    @property
    def _TABLE_NAME(self):
        return self.get_tablename(self._dbptr)
    
    @property
    def _PRIMARY_KEY(self):
        return self.get_primarykey(self._dbptr)
    
    @property
    def _TABLE_FIELDS(self):
        return self.get_tablefields(self._dbptr)
    
    @property
    def Fields(self):
        flist = list(self._TABLE_FIELDS)
        flist.sort()
        return flist
    #------------------------------------------------------------------------#
    
    @property
    def _nullptr(self):
        """
        Return NULL record for a given pointer
        """
        nullptr = Dbptr(self._dbptr)
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
            self._dbptr = Dbptr(db)
        else:
            self._dbptr = Dbptr()
            raise NotImplementedError("No empty contructor allowed here yet...")
        
        # If instance is being tied to a table, complain if not a pointer to that one
        if self.__tablename__ is not None and self.__tablename__ != self._TABLE_NAME:
            raise ValueError("Not a valid pointer for " +  self.__tablename__)
        
        self.__tablename__ = self._TABLE_NAME
        self.PRIMARY_KEY  = self._PRIMARY_KEY
        self.TABLE_FIELDS = self._TABLE_FIELDS

    def _get_null(self, field):
        """
        Returns NULL value for a given field
        """
        return self._nullptr.getv(field)[0]

    def get(self, field):
        """Get a database value from the given field (NULL supported)
        
        If the value is a NULL value for that field, return a python None
        """
        value = getattr(self, field)
        if value == self._get_null(field):
            value = None
        return value

    def set(self, field, value):
        """Set a database field to the given value (NULL supported)
        
        Setting a field to 'None' puts a NULL value in for that record field
        """
        if value is None:
            value = self._get_null(field)
        setattr(self, field, value)

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
        fields = tuple([getattr(self,f) for f in self.TABLE_FIELDS])
        return formatting % fields
    
    def to_dict(self):
        """
        Return a dict of key, value pairs for every field in the record
        
        Acts as a readonly '__dict__' variable

        """
        return dict([(f, self.get(f)) for f in self.TABLE_FIELDS])


class datascope_column(property):
    """
    Maps a column field from Datascope database to a property attribute
    
    Any access to this attribute will attempt to directly get and set values to
    this object's corresponding database field.

    Note: docstring doesn't work right now.
    """
    @staticmethod
    def _dbgetter(field):
        def _dbget(self):
            return self._dbptr.getv(field)[0]
        return _dbget
    
    @staticmethod
    def _dbsetter(field):
        def _dbset(self, value):
            self._dbptr.setv(field, value)
        return _dbset
    
    @staticmethod
    def _dbdeleter(field):
        def _dbdel(self, field):
            # set to none
            pass
        return _dbdel

    def __init__(self, field, table=None):
        """
        Build a property attribute for db mapping a field name:

        attribute_property_object = datascope_column(fieldname)
        
        Caveat
        ------
        The object you assign this attr object to MUST have the Dbptr instance
        as an attribute named '_dbptr', and it MUST point to a single record

        """
        super(datascope_column, self).__init__(self._dbgetter(field), self._dbsetter(field), self._dbdeleter(field), "Get '"+field+"' from current row object")


class RowProxy(object):
    """Build a row proxy from a Dbptr instance"""
    def __new__(cls, dbptr):
        tablename = dbptr.query('dbTABLE_NAME')
        fields    = dbptr.query('dbTABLE_FIELDS')
        row = type(cls.__name__, (_RowProxy,), { f : datascope_column(f.name, tablename) for f in fields} )     
        return row(dbptr)


class MappedRow(object):
    """Build a row proxy from a DBAPI2 Cursor instance and a row using row_factory syntax"""
    def __new__(cls, cursor, row):
        tablename = cursor._dbptr.query('dbTABLE_NAME')
        mappedrow = type(cls.__name__, (_RowProxy,), { f.name.replace('.','_') : datascope_column(f.name, tablename) for f in cursor.description} )     
        return mappedrow(cursor._dbptr)
        


