from sqlalchemy.dialects import registry

from odm.nosql import green, NoSqlDialect

from .engine import RedisEngine


class RedisDialect(NoSqlDialect):
    '''RethinkDB dialect
    '''
    engine_cls = RedisEngine

    def has_table(self, connection, table_name, schema=None):
        return False

    @green
    def initialize(self, connection):
        self.server_version_info = yield from connection.execute('INFO')


registry.register("redis", "odm.backends.redis", "RedisDialect")
