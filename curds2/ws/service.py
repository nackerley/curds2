#
"""
service curds2 requests
"""
import curds2.c_dbapi2 as dbapi2

class Service(object):
    """
    Run a curds2 query as a service
    """
    cursor_params = {}

    def __init__(self, dbname=None, cursor_params={}):
        """stub"""
        if dbname:
            self.dbname = dbname
        else:
            raise dbapi2.ProgrammingError("No database provided")
            
        if cursor_params:
            self.cursor_params = cursor_params

    def dbprocess(self, cmds):
        """
        connect to a db, run dbprocess, close connection
        """
        with dbapi2.connect(self.dbname) as conn:
            curs = conn.cursor(**self.cursor_params)
            nrecs = curs.execute('dbprocess', [cmds])
            desc = curs.description
            rows = [c for c in curs]
        return {'nrecs': nrecs, 'description': desc, 'rows': rows}

    def execute(self, args, method='dbprocess'):
        if not hasattr(self, method):
            raise AttributeError("No such method: {0}".format(method))    
        return getattr(self, method)(args)
