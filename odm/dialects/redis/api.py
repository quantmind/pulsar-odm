from functools import partial

import pulsar
from pulsar.apps.data import redis

from odm.nosql import (NoSqlApi, NoSqlConnection, TABLE_STATEMENTS, green,
                       NoSqlCursor)

from .scripts import RedisScript

EXECUTE_SCRIPT = 'EXECUTE_SCRIPT'
LOAD_SCRIPTS = 'LOAD_SCRIPTS'


class Connection(redis.RedisStoreConnection, NoSqlConnection):

    def cursor(self):
        return Cursor(self)


class Cursor(NoSqlCursor):

    def __init__(self, connection):
        self._result = None
        self.connection = connection

    @property
    def description(self):
        return self._result[0]

    def close(self):
        self.connection = None

    @green
    def execute(self, statement, parameters):
        if not self.connection:
            raise redis.RedisError
        args = ()
        if isinstance(parameters, tuple):
            args, parameters = parameters, {}
        if statement in TABLE_STATEMENTS:
            return
        elif statement == LOAD_SCRIPTS:
            result = yield from _load_scripts(self, *args, **parameters)
        elif statement == EXECUTE_SCRIPT:
            result = yield from _execute_script(self, *args, **parameters)
        else:
            result = yield from self.connection.execute(
                statement, *args, **parameters)
        self._result = result

    @green
    def executemany(self, statement, parameters):
        if not self.connection:
            raise redis.RedisError
        if statement in TABLE_STATEMENTS:
            return
        return self.connection.execute(statement, **parameters)


class DBAPI(NoSqlApi):
    protocol_factory = partial(Connection, redis.Consumer)
    Error = redis.RedisError
    ResponseError = redis.ResponseError

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parser_class = redis.redis_parser()

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


#    INTERNALS

def _load_scripts(cursor):
    pipe = redis.Pipeline(cursor.connection)
    for script in RedisScript._scripts.values():
        pipe.execute('SCRIPT LOAD', script.script)
    return pipe.commit()


def _execute_script(cursor, script=None, keys=None, args=None, options=None):
    s = RedisScript._scripts.get(script)
    if not s:
        raise redis.RedisError('No such script "%s"' % script)
    args = s.preprocess_args(cursor, args)
    numkeys = len(keys)
    keys_args = tuple(keys) + args
    result = yield from cursor.connection.execute(
        'EVALSHA', s.sha1, numkeys, *keys_args, **options)
    return s.callback(result, **options)
