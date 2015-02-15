'''Object Data Mapper for pulsar asynchronous framework'''
import os

VERSION = (0, 1, 0)

__version__ = '.'.join((str(v) for v in VERSION))
__author__ = "Luca Sbardella"
__contact__ = "luca@quantmind.com"
__homepage__ = "https://github.com/quantmind/pulsar-odm"


if os.environ.get('pulsar_odm_setup') != 'running':
    from pulsar.apps.data import (Store, RemoteStore, create_store,
                                  register_store, Command)
    from .errors import *
    from .fields import *
    from .relfields import *
    from .manager import Manager
    from .model import Model, create_model
    from .mapper import Mapper
    from . import backends
