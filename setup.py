#
# setup.py file for installing
#
from numpy.distutils.core import setup, Extension
import sys, os

s_args = {'name'         : 'psycods2',
          'version'      : '0.4.0',
          'description'  : 'DBAPI2 compatible module for Datascope',
          'author'       : 'Mark Williams',
          'url'          : 'https//github.com/markcwill',
          'packages'     : ['psycods2'],
}

# Go
setup(**s_args)
