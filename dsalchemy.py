import psycods2 as dbapi2

#class _generative(object):
#    """Mark a method as generative."""
#    def __init__(self, f):
#        print f
#        self.f = f
##    
#    def __call__(self, *args, **kwargs):
#        print args, kwargs
#        self.f(*args, **kwargs)
#        return self.f

#class Origin(object):
#    __tablename__ = 'origin'
#    
#    lat = None
#    lon = None
#    depth = None
#    time = None

class Url(object):
    #dburl is similar to a sqlalchemy URL where it can be any object
    def __init__(self, database=None, driver=None, username=None, password=None, host=None, port=None):
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password
        self.host = host
        self.port = port

class _Session(object):
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
    def configure(self,**kwargs):
        self.__dict__.update(kwargs)

    def __init__(self, **kwargs):
        self._connection = self._connection_for_bind(self.engine, **kwargs)
    
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
        """
        self.cursor.executemany('join', [[t] for t in props])    

    def outerjoin(self, *props, **kwargs):
        self.cursor.executemany('join', [(t, True) for t in props])    
        
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
        self.connect_args = {}
        self.__dict__.update(kwargs)
    
    def connect(self, **kwargs):
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
    def __new__(self, bind=None, class_=_Session):
        return type.__new__(type, "Session", (class_,), {'engine': bind, 'connect_args': bind.connect_args})
            
def create_engine(dburl, **kwargs):
    """
    Create a database Engine from a Url object

    Inputs
    ------
    dburl : Url instance with 'database' attribute set

    """
    url = dburl
    pool = None
    dialect = None
    return Engine(pool, dialect, url, **kwargs)

