'''Object Data Mapper for pulsar asynchronous framework'''
import os


VERSION = (0, 1, 0)

__version__ = '.'.join((str(v) for v in VERSION))
__author__ = "Luca Sbardella"
__contact__ = "luca@quantmind.com"
__homepage__ = "https://github.com/quantmind/pulsar-odm"


if os.environ.get('pulsar_odm_setup') != 'yes':
    from .mapper import Model, Mapper, OdmSession, logger, model_base
    from .strategy import create_engine
    from . import dialects
