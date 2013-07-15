DBAPI 2.0 Cursor for Datascope
==============================

This is a implementation of the DBAPI 2.0 database API standard for the Datascope database system designed by [Boulder Real Time Technologies](http://brtt.com).


Summary
-------

The purpose of this module is to abstract processing programs from any specific database backend. See the [PEP 249 -- Python Database API Specification v2.0](http://www.python.org/dev/peps/pep-0249/) for details on the specifications, and the python class, function and method docstrings for details on the implementation.


Implementation of Execute
-------------------------
The `execute` method of the `Cursor` currently accepts the string name of any Dbptr method. A subsequent sequence or mapping is then passed as arguments to this method on the current Cursor pointer. Variable substitution is currently not supported. Any execution returning a pointer sets this as the new cursor pointer and returns the number of records. All other returns values (i.e. queries) are returned directly.

As a non-standard convenience, one can run any Dbptr method directly on the 'execute' object as if it were a pointer:

### Example
```python
>>> from curds2 import connect
>>> curs = connect('/tmp/spam').cursor()

# Standard DBAPI2 style
>>> nrecs1 = curs.execute('lookup', {'table':'origin'})

# Full function call via native API:
>>> nrecs2 = curs.execute.lookup(table='origin')
>>> nrecs1 == nrecs2
True

```

Customizations
--------------

### NULL support

Datascope has no NULL type, each field defines its own value which compares equal to NULL. Therefore, NULLs must be explicitly looked up and converted, at a slight performance overhead. This can be enabled for this module by setting the Cursor attribute `CONVERT_NULL` to `True`. All fields in any rows returned the the `fetch*` methods will contain a python `None` for any NULL value. 

### Factory support

#### Cursor Factory
Custom Cursors are allowed by passing the class to the `cursor_factory` attribute of any Connection. Possible uses would be to inherit the current Cursor class and override an internal method for more efficient object-relational mapping of rows...

#### Row Factory
This module supports row factory classes similar to those of the sqlite3 (among others) implementation of the DBAPI. Instances of a Cursor or Connection have a attribute called `row_factory`. Setting this attribute to a special class constuctor which has the format: `GenericRowFactory(cursor, row)` allows for the custom building of rows. The default row returned by the `fetch*` methods is the standard `tuple`. Currently this module has several pre-defined row factory classes:
* NamedTupleRow - Rows of python namedtuples with attribute-style access to each item.
* OrderedDictRow - Rows of python OrderedDict instances.
* UTCOrdDictRow - Identical to OrderedDictRow, with any field comparing to 'dbTIME' converted to an ObsPy UTCDateTime object.
* SQLValuesRow - A namedtuple row of values converted to SQL strings, contains methods to generate SQL statement parts.


Contact
-------

Copyright 2013 Mark Williams at [Nevada Seismological Laboratory](http://www.seismo.unr.edu/Faculty/29)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.


