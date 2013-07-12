import os, unittest
from curds2.dbapi2 import connect, Connection, Cursor, _Executer

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
    

class CursorTestCase(unittest.TestCase):
    
    dsn = '/opt/antelope/data/db/demo/demo'
    
    def test_cursor(self):
        """Test for Cursor Constructor"""
        with connect(self.dsn) as conn:
            curs = conn.cursor()
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
        
    def test_cursor_close(self):
        """Test Cursor close"""
        # Test we are connected to the DB
        curs = Connection(self.dsn).cursor()
        dbptr = getattr(curs, '_dbptr')
        self.assertNotEqual( dbptr.query('dbDATABASE_COUNT'), 0 )
        curs.close() 
        self.assertEqual( dbptr.query('dbDATABASE_COUNT'), 0 )
        

if __name__ == '__main__':

    connection_suite = unittest.TestLoader().loadTestsFromTestCase(ConnectionTestCase)
    cursor_suite     = unittest.TestLoader().loadTestsFromTestCase(CursorTestCase)
    
    suite = unittest.TestSuite([connection_suite, cursor_suite])
    unittest.TextTestRunner(verbosity=3).run(suite)


