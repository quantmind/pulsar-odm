from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects import registry


class PGDGreen(PGDialect_psycopg2):
    '''PostgreSql dialect using psycopg2 and greenlet to obtain an
    implicit asynchronous database connection.
    '''
    @classmethod
    def dbapi(cls):
        from odm.backends.postgresql import async
        return async


registry.register("postgresql.async", "odm.backends.postgresql", "PGDGreen")
