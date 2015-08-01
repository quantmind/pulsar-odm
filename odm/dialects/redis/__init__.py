from sqlalchemy.dialects import registry

from odm.nosql import NoSqlDialect, get_loop


class RedisDialect(NoSqlDialect):
    '''Redis green dialect
    '''
    name = 'redis'
    is_green = True

    @classmethod
    def dbapi(cls):
        from odm.dialects.redis import api
        return api.DBAPI(get_loop())

    def has_table(self, connection, table_name, schema=None):
        return False

    def initialize(self, connection):
        self.server_version_info = connection.execute('INFO').first()
        return
        # self.scripts = connection.execute('LOAD_SCRIPTS')

    def database_create(self, engine, database):
        pass

    def database_drop(self, engine, database):
        pass


registry.register("redis.green", "odm.dialects.redis", "RedisDialect")
