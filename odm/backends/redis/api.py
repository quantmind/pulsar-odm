import pulsar
from pulsar.apps.data import redis

from odm.nosql import (NoSqlApi, NoSqlConnection, TABLE_STATEMENTS, green,
                       NoSqlCursor)


redis_parser = redis.redis_parser()


class Connection(pulsar.Connection, NoSqlConnection):

    def __init__(self, **kw):
        super().__init__(redis.Consumer, **kw)
        self.parser = redis_parser()

    def execute(self, *args, **options):
        consumer = self.current_consumer()
        consumer.start((args, options))
        result = yield from consumer.on_finished
        if isinstance(result, redis.ResponseError):
            raise result.exception
        return result

    def cursor(self):
        return Cursor(self)


class Cursor(NoSqlCursor):

    def __init__(self, connection):
        self.connection = connection

    @property
    def description(self):
        return None

    def close(self):
        self.connection = None

    @green
    def execute(self, statement, parameters):
        if not self.connection:
            raise redis.RedisError
        if statement in TABLE_STATEMENTS:
            return
        return self.connection.execute(statement, **parameters)

    @green
    def executemany(self, statement, parameters):
        if not self.connection:
            raise redis.RedisError
        if statement in TABLE_STATEMENTS:
            return
        return self.connection.execute(statement, **parameters)


class DBAPI(NoSqlApi):
    protocol_factory = Connection
    Error = redis.RedisError
    ResponseError = redis.ResponseError

    def connect(self, host=None, port=None, password=None, database=None,
                **kw):
        '''Connect to redis, authenticate and select the database
        '''
        if host and port:
            transport, connection = yield from self._loop.create_connection(
                self.create_protocol, host, port)
        else:
            raise NotImplementedError('Could not connect to %s' %
                                      str(self._host))
        if password:
            yield from connection.execute('AUTH', password)
        if database:
            yield from connection.execute('SELECT', database)
        return connection
