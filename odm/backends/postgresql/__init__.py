from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects import registry

from odm.pool import AsyncPool


class PGDGreen(PGDialect_psycopg2):
    '''PostgreSql dialect using psycopg2 and greenlet to obtain an
    implicit asynchronous database connection.
    '''
    poolclass = AsyncPool
    green_pool = None

    @classmethod
    def dbapi(cls):
        from odm.backends.postgresql import async
        return async

    def create_connect_args(self, url):
        args, opts = super().create_connect_args(url)
        opts.pop('pool_size', None)
        opts.pop('timeout', None)
        return [[], opts]

    def connect(self, *cargs, **cparams):
        ''''''
        return self.dbapi.green_connect(self.green_pool, *cargs, **cparams)


registry.register("postgresql.async", "odm.backends.postgresql", "PGDGreen")
