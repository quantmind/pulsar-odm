'''RethinkDB backend'''
import struct
from functools import partial

try:
    import rethinkdb
    from rethinkdb import ql2_pb2 as p
    from .protocol import Connection, Consumer
except ImportError:
    rethinkdb = None

from pulsar import Pool

import odm


class RethinkDB(odm.RemoteStore):
    '''RethinkDB data store
    '''
    protocol_factory = partial(Connection, Consumer)

    @property
    def registered(self):
        return rethinkdb is not None

    def create_database(self, dbname=None, **kw):
        '''Create a new database
        '''
        dbname = dbname or self.database
        connection = yield from self._pool.connect()
        yield from ast.DbCreate(dbname).run(connection)
        self._database = dbname

    def create_table(self, dbname=None, **kw):
        pass

    def execute(self, *args, **options):
        connection = yield from self._pool.connect()
        with connection:
            result = yield connection.execute(*args, **options)
            if isinstance(result, ResponseError):
                raise result.exception
            coroutine_return(result)

    def connect(self):
        '''Create a new connection to RethinkDB'''
        protocol_factory = self.create_protocol
        host, port = self._host
        transport, connection = yield from self._loop.create_connection(
                protocol_factory, host, port)
        handshake = connection.current_consumer()
        handshake.start(self.auth_key)
        yield from handshake.on_finished
        return connection

    def _init(self, pool_size=50, auth_key='', **kwargs):
        self.auth_key = auth_key.encode('ascii')
        self._pool = Pool(self.connect, pool_size=pool_size, loop=self._loop)


odm.register_store("rethinkdb", "odm.backends._rethinkdb.RethinkDB")
