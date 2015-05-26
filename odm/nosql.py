from functools import wraps

import sqlalchemy
from sqlalchemy.engine import Dialect, Connectable
from sqlalchemy import pool

from pulsar import get_actor
from pulsar.apps.greenio import wait


def green(callable):
    '''Decorator for green functions and methods
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        return wait(callable(*args, **kwargs))

    return _


def get_loop():
    return get_actor()._loop


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
