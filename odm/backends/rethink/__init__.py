from sqlalchemy.dialects import registry

from odm.nosql import NoSqlDialect, get_loop, green


class RethinkDialect(NoSqlDialect):
    '''Redis green dialect
    '''
    name = 'redis'
    is_green = True
    statement_compiler = RedisCompiled

    @classmethod
    def dbapi(cls):
        from odm.backends.rethink import api
        return api.DBAPI(get_loop())

    def has_table(self, connection, table_name, schema=None):
        return False

    def initialize(self, connection):
        self.server_version_info = connection.execute('INFO')

    @green
    def do_execute(self, cursor, statement, parameters, context=None):
        result = yield from cursor.execute(statement, **parameters)
        if isinstance(result, self.dbapi.ResponseError):
            raise result.exception
        return result

    @green
    def database_create(self, engine, database):
        return self.dbapi.database_create()

    def database_drop(self, engine, database):
        pass

    def item_create(self, compiled, params):
        raise NotImplementedError

    def item_update(self, compiled, params):
        raise NotImplementedError

    def item_delete(self, compiled, params):
        raise NotImplementedError


registry.register("redis.green", "odm.backends.redis", "RedisDialect")
