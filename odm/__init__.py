'''Object Data Mapper for pulsar asynchronous framework'''
VERSION = (0, 1, 0, 'alpha', 1)

from pulsar.utils.version import get_version
from pulsar.apps.data import Store, RemoteStore, create_store, register_store
from . import backends

__version__ = get_version(VERSION)
__author__ = "Luca Sbardella"
__contact__ = "luca@quantmind.com"
__homepage__ = "https://github.com/quantmind/pulsar-odm"
