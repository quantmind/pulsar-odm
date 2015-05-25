from functools import wraps

import sqlalchemy
from sqlalchemy.engine import Dialect, Connectable, url
from sqlalchemy.engine.strategies import PlainEngineStrategy
from sqlalchemy import pool

from pulsar import get_actor
from pulsar.apps.greenio import wait


def create_engine(*args, **kwargs):
    kwargs.setdefault('strategy', 'odm')
    return sqlalchemy.create_engine(*args, **kwargs)


def green(callable):
    '''Decorator for green functions and methods
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        return wait(callable(*args, **kwargs))

    return _


def get_loop():
    return get_actor()._loop


class OdmEngineStrategy(PlainEngineStrategy):
    name = 'odm'

    def create(self, name_or_url, **kwargs):
        # create url.URL object
        u = url.make_url(name_or_url)
        dialect_cls = u.get_dialect()

        if hasattr(dialect_cls, 'engine_cls'):
            dialect = dialect_cls(**kwargs)
            return dialect_cls.engine_cls(dialect, u)
        else:
            return super().create(name_or_url, **kwargs)

OdmEngineStrategy()


class Engine(Connectable):

    def __init__(self, dialect, url):
        self.url = url
        self.dialect = dialect
        args, opts = dialect.create_connect_args(url)
        self.args = args
        self.opts = opts

    def database_create(self, dbname):
        raise NotImplementedError

    def contextual_connect(self):
        return self.connect()

    def _run_visitor(self, visitorcallable, element, **kwargs):
        visitorcallable(self.dialect, self,
                        **kwargs).traverse_single(element)



class NoSqlDialect(Dialect):
    name = 'nosql'
    identifier_preparer = None

    def __init__(self, dbapi=None, **kw):
        self.dbapi = dbapi

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        opts.update(url.query)
        return [[], opts]

    def validate_identifier(self, name):
        pass

    def on_connect(self):
        return None

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(*cargs, **cparams)
