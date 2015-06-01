from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects import registry

from .pool import GreenletPool


class PGDGreen(PGDialect_psycopg2):
    '''PostgreSql dialect using psycopg2 and greenlet to obtain an
    implicit asynchronous database connection.
    '''
    poolclass = GreenletPool
    is_green = True

    @classmethod
    def dbapi(cls):
        from odm.dialects.postgresql import green
        return green

    def create_connect_args(self, url):
        args, opts = super().create_connect_args(url)
        opts.pop('pool_size', None)
        opts.pop('pool_timeout', None)
        return [[], opts]


registry.register("postgresql.green", "odm.dialects.postgresql", "PGDGreen")
