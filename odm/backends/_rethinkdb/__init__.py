'''RethinkDB backend'''
from functools import partial
from collections import OrderedDict

try:
    import rethinkdb
    from rethinkdb import ast
    from .protocol import Connection, Consumer, start_query
except ImportError:     # pragma    nocover
    rethinkdb = None

from pulsar import Pool

import odm

Command = odm.Command


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

    # Transaction
    def execute_transaction(self, transaction):
        updates = OrderedDict()
        inserts = OrderedDict()
        for command in transaction.commands:
            action = command.action
            if not action:
                raise NotImplementedError
            else:
                model = command.args
                table_name = model._meta.table_name
                data = dict(model._meta.store_data(model, self, action))
                group = inserts if action == Command.INSERT else updates
                if table_name not in group:
                    group[table_name] = [], []
                group[table_name][0].append(data)
                group[table_name][1].append(model)
        #
        for table, docs_models in inserts.items():
            executed = yield from self.update_documents(table, docs_models[0])

            errors = []
            for key, model in zip(executed['generated_keys'],
                                  docs_models[1]):
                model['id'] = key
                model['_rev'] = key
                model._modified.clear()

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

    def update_documents(self, table, documents, **kw):
        '''Bulk update/insert of documents in a database
        '''
        term = ast.Table(table).insert(documents, **kw)
        return self.execute(term)

    def _init(self, pool_size=50, auth_key='', **kwargs):
        self.auth_key = auth_key.encode('ascii')
        self._pool = Pool(self.connect, pool_size=pool_size, loop=self._loop)


odm.register_store("rethinkdb", "odm.backends._rethinkdb.RethinkDB")
