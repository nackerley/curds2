#
# SQLAlchemy-type classes for Datascope
# -by Mark 2013
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
#
# Notes: 
# - All classes EXCEPT for 'URL' written by Mark Williams
#
# - URL class was adapted from exisitng SQLAlchemy class (MIT license)
#   see inline for copyright info.

import re, urllib


class URL(object):
    # BASED ON SQLAlchemy, some methods were copied outright.
    # Their copyright and release is below:
    #----------------------------------------------------------------------------#
    # Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
    #
    # This module is part of SQLAlchemy and is released under
    # the MIT License: http://www.opensource.org/licenses/mit-license.php
    #----------------------------------------------------------------------------#
    """
    URL object describing a database
    
    """

    _defaults = {
        'datascope' : 'psycods2'
    }
    
    def __init__(self, drivername, username=None, password=None, host=None, port=None, database=None, query=None):
        self.drivername = drivername
        self.username = username
        self.password = password
        self.host = host
        if port is not None:
            self.port = int(port)
        else:
            self.port = None
        self.database = database
        self.query = query or {}

    def __str__(self):
        s = self.drivername + "://"
        if self.username is not None:
            s += self.username
            if self.password is not None:
                s += ':' + urllib.quote_plus(self.password)
            s += "@"
        if self.host is not None:
            s += self.host
        if self.port is not None:
            s += ':' + str(self.port)
        if self.database is not None:
            s += '/' + self.database
        if self.query:
            keys = self.query.keys()
            keys.sort()
            s += '?' + "&".join("%s=%s" % (k, self.query[k]) for k in keys)
        return s

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return \
            isinstance(other, URL) and \
            self.drivername == other.drivername and \
            self.username == other.username and \
            self.password == other.password and \
            self.host == other.host and \
            self.database == other.database and \
            self.query == other.query
    
    def get_dialect(self):
        """Stub"""
        if '+' in self.drivername:
            name, driver = self.drivername.split('+')
        else:
            name = self.drivername
            driver = self._defaults[name]
        modname = '__'.join([name, driver])
        clsname = '__'.join([name.capitalize(), driver.capitalize()])
        mod = __import__(modname)
        cls = getattr(mod, clsname)
        return cls
    
    @staticmethod
    def _parse_rfc1738_args(name):
        pattern = re.compile(r'''
                (?P<name>[\w\+]+)://
                (?:
                    (?P<username>[^:/]*)
                    (?::(?P<password>[^/]*))?
                @)?
                (?:
                    (?P<host>[^/:]*)
                    (?::(?P<port>[^/]*))?
                )?
                (?:/(?P<database>.*))?
                ''', re.X)

        m = pattern.match(name)
        if m is not None:
            components = m.groupdict()
            if components['database'] is not None:
                tokens = components['database'].split('?', 2)
                components['database'] = tokens[0]
                query = (len(tokens) > 1 and tokens[1].split('&') ) or None
                # Py2K
                if query is not None:
                    query = dict((k.encode('ascii'), query[k]) for k in query)
                # end Py2K
            else:
                query = None
            components['query'] = query

            if components['password'] is not None:
                components['password'] = \
                    urllib.unquote_plus(components['password'])

            name = components.pop('name')
            return URL(name, **components)
        else:
            raise ValueError(
                "Could not parse rfc1738 URL from string '%s'" % name)

    @classmethod
    def from_string(cls, url_string):
        return cls._parse_rfc1738_args(url_string)


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
        cursor.executemany('putv', [(k,v) for k,v in obj.to_dict.iteritems() if k in fields])


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
    def __new__(cls, bind=None, class_=_SessionMethods):
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
    if isinstance(url, str):
        url = URL.from_string(url)
    # Stubs for now - typically build pool/dialect from url
    pool    = None
    dialect = None

    if dialect is None:
        dialect = url.get_dialect()
    return Engine(pool, dialect, url, **kwargs)


