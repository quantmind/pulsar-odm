from rethinkdb import ast

import pulsar

from odm.nosql import NoSqlApi


class DBAPI(NoSqlApi):

    def database_create(self, connection, database, **kw):
        term = ast.DbCreate(database)
        result = yield from self.execute(connection, term, **kw)
        assert result['dbs_created'] == 1
        return result

    def execute(self, connection, term, **options):
        consumer = connection.current_consumer()
        options['db'] = ast.DB(self.database)
        query = start_query(term, connection.requests_processed, options)
        consumer.start(query)
        response = yield from consumer.on_finished
        return response
