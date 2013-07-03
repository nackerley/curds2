#
# datascope_psycods2 -by MCW 2013-07-01
#
"""
SQLAlchemy-type Dialect using the DBAPI2 module
psycods2 as the driver.

This implements an ORM for datascope DBAPI by using
a special row_factory class which generates rows with
active pointers to the database they are connected to.

Main classes
------------
_BaseRow -> row_factory class for DBAPI2 which creates ORM rows

Dialect -> stub for SQLAlchemy compatibility

Datascope__Psycods2 -> Dialect for this database/driver combo

"""

from orm import RowProxy

class _BaseRow(object):
    """
    Row factory generator for DBAPI2 using ORM class.
    
    Uses the RowProxy to generate a row as an
    instance of a Base class of type 'table', where 'table'
    is the relation you are working with.

    Notes
    -----
    Constructor as other row_factory classes for DBAPI2:
    row = _BaseRow(cursor, row)

    TEST - should work but not efficient implementation...
    e.g., 'row' is currently not used, so pulling out those
    values is technically unnecessary work for fetch*. The
    upside is, it's drop-in compatible with DBAPI2...
    (override Cursor would be best??)
    
    Example
    -------
    >>> curs = connect('/opt/antelope/data/db/demo/demo').cursor(row_factory=_BaseRow)
    >>> nrec = curs.execute('lookup', {'table':'site'})
    >>> curs.fetchone()
    Site(sta=HIA, ondate=1986201, offdate=None)
    
    """
    def __new__(cls, cursor, row):
        return RowProxy(cursor._dbptr)

class Dialect(object):
    """Stub Dialect"""
    name = None
    driver = None


class Datascope__Psycods2(Dialect):
    """
    Dialect for Datascope using the DBAPI2.0
    """
    
    name = 'datascope'
    driver = 'psycods2'
    
    @staticmethod        
    def create_connect_args(url):
        conn_args = {'row_factory' : _BaseRow, 'CONVERT_NULL' : False}
        if 'perm' in url.query:
            conn_args.update({'perm': url.query['perm']})
        return conn_args
