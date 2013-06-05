#
# SQLAlchemy-type classes for Datascope
#

#
# HIGHLY EXPERIMENTAL
# This is a hacktacular quickie attempt at stubbing and placeholding
# various classes so one can use a Datascope database with the same
# syntax as an SQL one, using SQLAlchemy-style.
#
# Theoretically, the only difference would be the URL:
#
# >>> eng = create_engine('datascope:////path/to/my/database')
#
# ...or something similar
#
# These are in no way complete, and bypass much of the connection and
# translation layer issues, for better or worse, due to the lack of
# multithreading connections and two-phase support for out-of-the-box
# Datascope stuff available in the basic pointer API
#

class Base(object):
    """
    Base class
    
    Not Implemented, could be done as a metaclass call based on schema?

    """
    def __init__(self, tablename=None):
        self.__tablename__ = tablename


class Dialect(object):
    """Stub Dialect"""
    pass


class Datascope_Psycods2(Dialect):
    """
    Dialect for Datascope using the DBAPI2.0
    """
    
    name = 'datascope'
    driver = 'psycods2'
    
    def __init__(self):
        self.drv = __import__(self.driver)
        
    def create_connect_args(self, url):
        row = getattr(self.drv, 'NamedTupleRow')
        return {'row_factory' : row, 'CONVERT_NULL' : True}


class Url(object):
    """URL object describing a database"""
    _defaults = {
        'datascope' : 'psycods2'
    }
    
    def __init__(self, database=None, drivername=None, username=None, password=None, host=None, port=None, query=None):
        self.database   = database
        self.drivername = drivername
        self.username   = username
        self.password   = password
        self.host       = host
        self.port       = port
        self.query      = query
    
    def get_dialect(self):
        """Stub"""
        if '+' in self.drivername:
            name, driver = self.drivername.split('+')
        else:
            name = self.drivername
            driver = self._defaults[name]
        clsname = '_'.join([name.capitalize(), driver.capitalize()])
        return globals()[clsname]()


class _SessionMethods(object):
    """
    Alchemy-style session class for Datascope
    
    This is a base class, to be inherited. Attributes must be set by a
    metaclass constructor-type like 'sessionmaker'

    Attributes
    ----------
    engine : Engine instance
             Must provide a 'connect' method which returns a
             DBAPI Connection
    """
    
    @classmethod
    def configure(cls, **kwargs):
        cls.__dict__.update(kwargs)

    def __init__(self, **kwargs):
        if kwargs:
            self.configure(**kwargs)
        if hasattr(self, 'connect_args'):
            args = self.connect_args
        self._connection = self._connection_for_bind(self.engine, **args)
    
    def _connection_for_bind(self, engine, **kwargs):
        return engine.connect(**kwargs)

    def query(self, cls):
        return Query(cls, session=self)
    
    def add(self, obj):
        cursor = self._connection.cursor(table=obj.__tablename__)
        rec = cursor.execute('addnull')
        cursor.scroll(rec, "absolute")
        fields = [d[0] for d in cursor.description]
        cursor.executemany('addv', [(k,v) for k,v in obj.__dict__.iteritems() if k in fields])


class Query(object):
    """
    Alchemy-style query for Datascope
    

    Attributes
    ----------
    session : Session instance of Query
    cursor  : DBAPI cursor instance from a Session Connection

    """
    session = None
    cursor = None
    
    def __init__(self, cls, session=None):
        """
        Create a Query

        Inputs
        ------
        cls = str or object with a '__tablename__' attribute str name of a table to query
        session = Session instance to use (created from sessionmaker) [None]

        """
        self.session = session
        if not isinstance(cls, str):
            cls = cls.__tablename__
        self.cursor = self.session._connection.cursor(table=cls)
    
    def __iter__(self):
        return self.cursor.__iter__()

    #def __str__(self):
    #    pass 
    
    def count(self):
        return self.cursor.rowcount

    def delete(self):
        """
        Not Implemented

        Should delete everything from query if possible
        """
        #for t in self:
        #    rc = self.cursor.execute('mark')
        #self.cursor.execute('crunch')
        pass

    def get(self, ident):
        if isinstance(ident,int):
            self.cursor.scroll(ident,'absolute')
            return self.cursor.fetchone()
        else:
            pass
    
    def join(self, *props, **kwargs):
        """
        Join stuff - needs work
        (kwargs should modify join keys, if possible)
        """
        self.cursor.executemany('join', [[t] for t in props])    
        return self

    def outerjoin(self, *props, **kwargs):
        self.cursor.executemany('join', [(t, True) for t in props])    
        return self

    def first(self):
        self.cursor.scroll(0, 'absolute')
        return self.cursor.fetchone()
    
    def one(self):
        if self.cursor.rowcount != 1:
            raise ValueError("More than one row returned!")
        else:
            return self.cursor.fetchone()

    def all(self):
        return [t for t in self]
    
    def filter(self, filter_string):
        self.cursor.execute('subset',[filter_string] )
        return self

    def filter_by(self,**kwargs):
        filter_str = ' && '.join([str(k)+'=='+str(v) for k,v, in kwargs.iteritems()])
        return self.filter(filter_str)

    def order_by(self, *args):
        'Might be wrong'
        self.cursor.execute('sort', args)
        return self


class Engine(object):
    """
    Engine which holds interface info for a database Session/Connection
    
    """
    connect_args = None
    
    def __init__(self, pool=None, dialect=None, url=None, **kwargs):
        self.pool = pool
        self.dialect = dialect
        self.url = url
        self.engine = self
        self.connect_args = dialect.create_connect_args(url)
        self.__dict__.update(kwargs)
    
    def connect(self, **kwargs):
        """
        Return a Connection to the database of this Engine

        Notes
        -----
        Uses engine Dialect, pass kwargs to Connection constructor

        """
        # fix this with a Dialect object later...
        dbapi2 = __import__(self.dialect.driver)
        if self.connect_args:
            connect_args = self.connect_args
        else:
            connect_args = {}
        if kwargs:
            connect_args.update(kwargs)
        return dbapi2.connect(self.url.database, **connect_args)


class sessionmaker(object):
    """
    Creates a Session class to spawn sessions from an Engine
    """
    # turn bind into a db name and perm?
    def __new__(self, bind=None, class_=_SessionMethods):
        return type.__new__(type, "Session", (class_,), {'engine': bind, 'connect_args': bind.connect_args})


def create_engine(dburl, **kwargs):
    """
    Create a database Engine from a Url object

    Inputs
    ------
    dburl : Url instance with 'database' attribute set

    """
    # set from URL
    url = dburl
    # Stubs for now - typically build pool/dialect from url
    pool    = None
    dialect = None

    if dialect is None:
        dialect = url.get_dialect()
    return Engine(pool, dialect, url, **kwargs)


