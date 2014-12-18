#
"""
Core DBAPI2 standards and modules
"""
import datetime
import logging

# DBAPI top level attributes
apilevel     = "2.0"      # 1.0 or 2.0
threadsafety = 0          # Playing it safe (datascope??)
paramstyle   = "format"   # N/A right now, execute uses Dbptr API


LOG = logging.getLogger(__name__)
try:
    LOG.addHandler(logging.NullHandler())
except:
    logging.raiseExceptions = False

# DBAPI standard exceptions
class Error(StandardError): 
    pass

class Warning(StandardError):
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

# DBAPI Type Objects / Functions
#----------------------------------------------------------------------------#
class DBAPITypeObject:
    def __init__(self,*values):
        self.values = values
    
    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

Binary = buffer
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
TimestampFromTicks = Timestamp.utcfromtimestamp

def DateFromTicks(ticks):
    return Date(TimestampFromTicks(ticks).timetuple()[:3])

def TimeFromTicks(ticks):
    return Time(TimestampFromTicks(ticks).timetuple()[3:6])

