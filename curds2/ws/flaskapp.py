#
"""
Flask app to service dbapi2 Antelope requests using curds2
"""
from flask import Flask
import gevent

from service import Service
