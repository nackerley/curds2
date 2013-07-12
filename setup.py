#
# setup.py file for installing
#
from numpy.distutils.core import setup, Extension

s_args = {'name'         : 'curds2',
          'version'      : '0.4.0',
          'description'  : 'DBAPI2 compatible module for Datascope',
          'author'       : 'Mark Williams',
          'url'          : 'https//github.com/markcwill',
          'packages'     : ['curds2'],
}

# Go
setup(**s_args)
