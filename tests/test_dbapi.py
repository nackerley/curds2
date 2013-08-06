import os, unittest
from curds2.dbapi2 import connect, Connection, Cursor, _Executer, ProgrammingError
from antelope.datascope import Dbptr

demo_origin_record_0 = (40.074, 69.164, 155.166, 704371900.66886, 1, -1, 1992118, 7, 7, -1, 715, 48, '-', '', -999.0, 'f', 2.62, 1, -999.0, -1, -999.0, -1, 'locsat:kyrghyz', 'JSPC', -1, 790466871.0)

demo_origin_assoc_fields = ['lat', 'lon', 'depth', 'time', 'orid', 'evid', 'jdate', 'nass', 'ndef', 'ndp', 'grn', 'srn', 'etype', 'review', 'depdp', 'dtype', 'mb', 'mbid', 'ms', 'msid', 'ml', 'mlid', 'algorithm', 'auth', 'commid', 'lddate', 'arid', 'assoc.orid', 'sta', 'phase', 'belief', 'delta', 'seaz', 'esaz', 'timeres', 'timedef', 'azres', 'azdef', 'slores', 'slodef', 'emares', 'wgt', 'vmodel', 'assoc.commid', 'assoc.lddate']

class ConnectionTestCase(unittest.TestCase):
    
    dsn = '/opt/antelope/data/db/demo/demo'

    def test_connect(self):
        """Test for connect function"""
        
        conn = connect(self.dsn)
        self.assertIsInstance(conn, Connection)
        conn.close()

    def test_connection(self):
        """Agile-type Connection Constructor test"""
        
        # Test object was built to spec
        conn = Connection(self.dsn)
        self.assertTrue( hasattr( conn, '_dbptr') )
        self.assertTrue( hasattr( conn, 'CONVERT_NULL') )
        self.assertTrue( hasattr( conn, 'cursor_factory') )
        self.assertTrue( hasattr( conn, 'row_factory') )
        self.assertTrue( hasattr( conn, '__init__') )
        self.assertTrue( hasattr( conn, '__enter__') )
        self.assertTrue( hasattr( conn, '__exit__') )
        self.assertTrue( hasattr( conn, 'close') )
        self.assertTrue( hasattr( conn, 'cursor') )
        
        # Test we are connected to the DB
        dbptr = getattr(conn, '_dbptr')
        self.assertNotEqual( dbptr.query('dbDATABASE_COUNT'), 0 )
        conn.close() 
        self.assertEqual( dbptr.query('dbDATABASE_COUNT'), 0 )

    def test_connection_context(self):
        """Test for Connection context manager methods"""

        with connect(self.dsn) as conn:
            dbptr = getattr(conn, '_dbptr')
            self.assertNotEqual( dbptr.query('dbDATABASE_COUNT'), 0 )
            
        self.assertEqual( dbptr.query('dbDATABASE_COUNT'), 0 )
        
    def test_connection_cursor(self):
        """Test for Connection cursor function"""
        with connect(self.dsn) as conn:
            self.assertIsInstance( conn.cursor(), Cursor )
    
    def test_cursor_close(self):
        """Test Cursor close"""
        # Test we are connected to the DB
        curs = Connection(self.dsn).cursor()
        dbptr = getattr(curs, '_dbptr')
        self.assertNotEqual( dbptr.query('dbDATABASE_COUNT'), 0 )
        curs.close() 
        self.assertEqual( dbptr.query('dbDATABASE_COUNT'), 0 )


class CursorTestCase(unittest.TestCase):
    
    def setUp(self):
        self.dsn = '/opt/antelope/data/db/demo/demo'
        self.NRECS_ORIGIN = 1351
        self.conn = connect(self.dsn)
        self.curs = self.conn.cursor()
    
    def test_cursor(self):
        """Test for Cursor Constructor"""
        curs = self.curs
        self.assertTrue( hasattr( curs, '_dbptr') )
        self.assertTrue( hasattr( curs, 'CONVERT_NULL') )
        self.assertTrue( hasattr( curs, 'row_factory') )
        self.assertTrue( hasattr( curs, 'connection') )
        self.assertTrue( hasattr( curs, 'rownumber') )
        self.assertTrue( hasattr( curs, 'rowcount') )
        self.assertTrue( hasattr( curs, '_nullptr') )
        self.assertTrue( hasattr( curs, 'description') )
        self.assertTrue( hasattr( curs, 'execute') )
        self.assertTrue( hasattr( curs, 'executemany') )
        self.assertTrue( hasattr( curs, '_fetch') )
        self.assertTrue( hasattr( curs, 'fetchone') )
        self.assertTrue( hasattr( curs, 'fetchmany') )
        self.assertTrue( hasattr( curs, 'fetchall') )
        self.assertTrue( hasattr( curs, 'scroll') )
        self.assertTrue( hasattr( curs, '__init__') )
        self.assertTrue( hasattr( curs, '__iter__') )
        
    def test_cursor_connection(self):
        self.assertIsInstance( self.curs.connection, Connection)

    def test_cursor_executer(self):
        self.assertIsInstance( self.curs.execute, _Executer )

    def test_dbptr(self):
        self.assertIsInstance( self.curs._dbptr, Dbptr )
    
    def test_rownumber(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        self.curs._dbptr.record = 5
        self.assertEqual(self.curs.rownumber, 5)

    def test_rowcount(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        self.assertEqual(self.curs._dbptr.nrecs(), self.curs.rowcount)

    def test_description(self):
        nrecs0 = self.curs.execute('process', [('dbopen origin', 'dbjoin assoc')])
        self.curs.scroll(0, 'absolute')
        names = [d[0] for d in self.curs.description]
        self.assertEqual(names, demo_origin_assoc_fields)

    def test_execute(self):
        """Stub for execute, see ExecuterTestCase"""
        pass

    def test_executemany(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        nrecs1 = self.curs.executemany('join', [('assoc', True), ('arrival', True)])        
        tables_in_join = self.curs.execute.query('dbVIEW_TABLES')
        self.assertTrue('origin' in tables_in_join)
        self.assertTrue('assoc' in tables_in_join)
        self.assertTrue('arrival' in tables_in_join)

    def test_fetch(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        self.curs.scroll(0, 'absolute')
        tup = self.curs._fetch()
        self.assertEqual(tup, demo_origin_record_0)
        self.assertEqual(self.curs.rownumber, 1)

    def test_fetchone(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        self.curs.scroll(0, 'absolute')
        tup = self.curs.fetchone()
        self.assertEqual(tup, demo_origin_record_0)
        self.assertEqual(self.curs.rownumber, 1)
    
    def test_fetchmany(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        self.curs.scroll(0, 'absolute')
        seq = self.curs.fetchmany(5)
        self.assertEqual(len(seq), 5)
        self.assertEqual(seq[0], demo_origin_record_0)
        self.assertEqual(self.curs.rownumber, 5)
        
    def test_fetchall(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        self.curs.scroll(4, 'absolute')
        seq = self.curs.fetchall()
        self.assertEqual(len(seq), self.NRECS_ORIGIN-4)

    def test_scroll(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        self.curs.scroll(5, 'absolute')
        self.assertEqual(self.curs.rownumber, 5)
        self.curs.scroll(2, 'relative')
        self.assertEqual(self.curs.rownumber, 7)

    def test_iter(self):
        nrecs0 = self.curs.execute('lookup', {'table':'origin'})
        rows = [r for r in self.curs]
        self.assertEqual(len(rows), nrecs0)
        self.curs.scroll(0, 'absolute')
        row0 = self.curs.fetchone()
        self.assertEqual(row0, rows[0])

    def tearDown(self):
        self.conn.close()


class ExecuterTestCase(unittest.TestCase):

    def setUp(self):
        self.dsn = '/opt/antelope/data/db/demo/demo'
        self.NRECS_ORIGIN = 1351
        self.conn = connect(self.dsn)
        self.curs = self.conn.cursor()

    def test_constructor(self):
        """Test Executer constructor"""
        ex = _Executer(self.curs)
        self.assertTrue( hasattr( ex, '__init__') )
        self.assertTrue( hasattr( ex, '__call__') )
        self.assertTrue( hasattr( ex, '__getattr__') )
        self.assertTrue( hasattr( ex, '__execute') )
        self.assertTrue( hasattr( ex, '__cursor') )

    def test_execute(self):
        """Test internal execute function"""
        nrecs0 = _Executer._Executer__execute(self.curs, 'lookup', table='origin')
        self.assertEqual(nrecs0, self.NRECS_ORIGIN)

    def test_call(self):
        """Test Executer hits execute using  __call__"""
        nrecs1 = self.curs.execute('lookup', {'table':'origin'})
        self.assertEqual(nrecs1, self.NRECS_ORIGIN)

    def test_getattr(self):
        """Test Executer hits execute using __getattr__"""
        nrecs2 = self.curs.execute.lookup(table='origin')
        self.assertEqual(nrecs2, self.NRECS_ORIGIN)
    
    def test_bad_method(self):
        """Test passing a non-existant module function"""
        self.assertRaises(ProgrammingError, self.curs.execute, 'spam_the_db', [])

    def tearDown(self):
        self.conn.close()


if __name__ == '__main__':

    connection_suite = unittest.TestLoader().loadTestsFromTestCase(ConnectionTestCase)
    cursor_suite     = unittest.TestLoader().loadTestsFromTestCase(CursorTestCase)
    executer_suite   = unittest.TestLoader().loadTestsFromTestCase(ExecuterTestCase)

    suite = unittest.TestSuite([connection_suite, cursor_suite, executer_suite])
    unittest.TextTestRunner(verbosity=2).run(suite)


