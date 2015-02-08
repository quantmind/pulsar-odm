'''RethinkDB backend'''
from functools import partial

try:
    import rethinkdb
    from rethinkdb import ast
    from .protocol import Connection, Consumer, start_query
except ImportError:     # pragma    nocover
    rethinkdb = None

from pulsar import Pool

import odm


class RethinkDB(odm.RemoteStore):
    '''RethinkDB asynchronous data store
    '''
    protocol_factory = partial(Connection, Consumer)

    @property
    def registered(self):
        return rethinkdb is not None

    # Database API
    def database_create(self, dbname=None, **kw):
        '''Create a new database
        '''
        term = ast.DbCreate(dbname or self.database)
        result = yield from self.execute(term, **kw)
        assert result['dbs_created'] == 1
        self.database = result['config_changes'][0]['new_val']['name']
        return result

    def database_all(self):
        '''The list of all databases
        '''
        return self.execute(ast.DbList())

    def database_drop(self, dbname=None, **kw):
        return self.execute(ast.DbDrop(dbname or self.database), **kw)

    # Table API
    def table_create(self, table_name, **kw):
        '''Create a new table
        '''
        return self.execute(ast.TableCreateTL(table_name), **kw)

    def table_all(self):
        '''The list of all tables in the current :attr:`~Store.database`
        '''
        return self.execute(ast.TableListTL())

    def table_drop(self, table_name, **kw):
        '''Create a new table
        '''
        return self.execute(ast.TableDropTL(table_name), **kw)

    # Execute a command
    def execute(self, term, dbname=None, **options):
        connection = yield from self._pool.connect()
        with connection:
            consumer = connection.current_consumer()
            db = dbname or self.database
            options['db'] = ast.DB(dbname or self.database)
            query = start_query(term, connection.requests_processed, options)
            consumer.start(query)
            response = yield from consumer.on_finished
            return response

    def connect(self):
        '''Create a new connection to RethinkDB server.

        This method should not be called directly unless a detached connection
        from the connection pool is needed.
        '''
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
