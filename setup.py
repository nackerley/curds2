#
# setup.py file for installing
#
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

s_args = {'name'         : 'curds2',
          'version'      : '0.5.5',
          'description'  : 'DBAPI2 compatible module for Datascope',
          'author'       : 'Mark Williams',
          'url'          : 'https//github.com/NVSeismoLab/curds2',
          'packages'     : ['curds2'],
}

# Go
setup(**s_args)
