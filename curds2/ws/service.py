#
"""
service curds2 requests
"""
import curds2.raw.dbapi2 as dbapi2


class Service(object):
    """
    Run a curds2 query as a JSONRPC service
    """
    cursor_params = {}

    def __init__(self, dbname=None, cursor_params={}):
        """stub"""
        if dbname:
            self.dbname = dbname.encode()
        else:
            raise dbapi2.ProgrammingError("No database provided")
            
        if cursor_params:
            self.cursor_params = cursor_params

    def dbprocess(self, cmds):
        """
        connect to a db, run dbprocess, close connection
        """
        cmds = [c.encode() for c in cmds if isinstance(c, unicode)]  # no Unicode support sux
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
    
    @classmethod
    def run(cls, request):
        """
        Turn a JSONRPC dict request into a JSONRPC dict reply
        """
        try:
            meth = request.get('method', 'dbprocess')
            params = request.pop('params')
            dbname = params.get('dbname', '')
            cmds = params.get('commands', [])
            curs = params.get('cursor', {})
            result = cls(dbname, curs).execute(cmds, method=meth)
            request.update({'result': result})
        except Exception as e:
            request.update({'error': {'message': e.message, 'type': e.__class__.__name__}})
        return request
