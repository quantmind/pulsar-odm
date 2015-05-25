'''Object Data Mapper for pulsar asynchronous framework'''
import os

from .mapper import Model, Mapper, logger
from .nosql import create_engine
from . import backends


VERSION = (0, 1, 0)

__version__ = '.'.join((str(v) for v in VERSION))
__author__ = "Luca Sbardella"
__contact__ = "luca@quantmind.com"
__homepage__ = "https://github.com/quantmind/pulsar-odm"
