'''Object Data Mapper for pulsar asynchronous framework'''
import os


VERSION = (0, 2, 0, 'final', 0)

__version__ = '.'.join((str(v) for v in VERSION))
__author__ = "Luca Sbardella"
__contact__ = "luca@quantmind.com"
__homepage__ = "https://github.com/quantmind/pulsar-odm"


if os.environ.get('pulsar_odm_setup') != 'yes':
    from pulsar.utils.version import get_version

    from .mapper import (Model, Mapper, OdmSession, logger, model_base,
                         table_args)
    from .strategy import create_engine
    from . import dialects

    __version__ = get_version(VERSION)

    __all__ = ['Model', 'Mapper', 'OdmSession', 'logger', 'model_base',
               'table_args', 'create_engine', 'dialects']
