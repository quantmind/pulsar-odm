from functools import wraps

import sqlalchemy
from sqlalchemy.engine import Dialect, Connectable, ResultProxy
from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.sql import compiler, crud
from sqlalchemy import pool, util

from greenlet import getcurrent

from pulsar import get_actor, Pool, Producer

CREATE = 'CREATE'
UPDATE = 'UPDATE'
DELETE = 'DELETE'
CREATE_TABLE = 'CREATE_TABLE'
DROP_TABLE = 'DROP_TABLE'
TABLE_STATEMENTS = frozenset((CREATE_TABLE, DROP_TABLE))


def green(callable):
    '''Decorator for green functions and methods
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        current = getcurrent()
        parent = current.parent
        assert parent, 'running in main greenlet'
        return parent.switch(callable(*args, **kwargs))

    return _


def get_loop():
    return get_actor()._loop


class GreenPool(pool.Pool):

    def __init__(self, creator, pool_size=10, timeout=30, **kw):
        self._pool = Pool(creator, pool_size=pool_size, timeout=timeout)
        super().__init__(green(self._pool.connect), **kw)

    def _do_get(self):
        return self._create_connection()

    def _do_return_conn(self, conn):
        self._pool._put(conn.connection)


class NoSqlConnection:
    '''Abstract class for Connections
    '''
    def rollback(self):
        pass

    def commit(self):
        pass

    def cursor(self):
        raise NotImplementedError


class NoSqlCursor:
    '''Abstract class for Cursors
    '''
    def close(self):
        raise NotImplementedError

    def execute(self, statement, parameters):
        raise NotImplementedError

    def executemany(self, statement, parameters):
        raise NotImplementedError


class NoSqlCompiler(compiler.Compiled):
    '''Compiler for NoSql datatores
    '''
    isdelete = isinsert = isupdate = False
    label_length = None
    crud_params = None

    def __init__(self, dialect, statement, column_keys=None,
                 inline=False, **kwargs):
        """Construct a new ``DefaultCompiler`` object.
        dialect
          Dialect to be used
        statement
          ClauseElement to be compiled
        column_keys
          a list of column names to be compiled into an INSERT or UPDATE
          statement.
        """
        self.column_keys = column_keys
        self.inline = inline
        self.binds = {}
        # self.bind_names = util.column_dict()
        self._result_columns = []
        self._ordered_columns = True
        super().__init__(dialect, statement, **kwargs)

    @property
    def sql_compiler(self):
        return self

    @util.memoized_property
    def _key_getters_for_crud_column(self):
        return crud._key_getters_for_crud_column(self)

    def construct_params(self, params=None, **kw):
        return params

    def format_table(self, table, use_schema=True, name=None):
        if name is None:
            name = table.name
        if use_schema and getattr(table, "schema", None):
            name = table.schema + "." + name
        return name

    @util.memoized_property
    def _bind_processors(self):
        processors = {}
        if self.crud_params:
            for param, _ in self.crud_params:
                processor = self.dialect.processor(param)
                if processor:
                    processors[param.name] = processor
        return processors

    # SUPPORTED VISITORS
    def visit_bindparam(self, bindparam, **kw):
        pass

    def visit_column(self, column, **kw):
        pass

    def visit_insert(self, statement, **kw):
        self.isinsert = True
        self.crud_params = crud._get_crud_params(self, statement, **kw)
        return CREATE

    def visit_update(self, statement, **kw):
        self.isupdate = True
        self.crud_params = crud._get_crud_params(self, statement, **kw)
        return UPDATE

    def visit_create_table(self, create):
        self.tablename = self.format_table(create.element)
        return CREATE_TABLE

    def visit_delete(self, statement, **kw):
        self.isdelete = True
        self.crud_params = crud._get_crud_params(self, statement, **kw)
        return DELETE

    def visit_drop_table(self, drop):
        self.tablename = self.format_table(drop.element)
        return DROP_TABLE


class NoSqlDialect(Dialect):
    name = 'nosql'
    identifier_preparer = None
    supports_alter = False
    positional = False
    supports_unicode_statements = True
    implicit_returning = False
    postfetch_lastrowid = True
    max_identifier_length = 9999
    label_length = None
    execute_sequence_format = tuple
    execution_ctx_cls = DefaultExecutionContext
    ddl_compiler = NoSqlCompiler
    statement_compiler = NoSqlCompiler

    def __init__(self, dbapi=None, **kw):
        self.dbapi = dbapi
        self.paramstyle = self.dbapi.paramstyle

    @classmethod
    def get_pool_class(cls, url):
        return getattr(cls, 'poolclass', GreenPool)

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        opts.update(url.query)
        return [[], opts]

    def processor(self, element):
        pass

    def validate_identifier(self, ident):
        pass

    def on_connect(self):
        return None

    def do_begin(self, dbapi_connection):
        pass

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_close(self, dbapi_connection):
        dbapi_connection.close()

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(*cargs, **cparams)

    def set_connection_execution_options(self, connection, opts):
        pass

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.executemany(statement, parameters)

    def do_execute(self, cursor, statement, parameters, context=None):
        cursor.execute(statement, parameters)

    def do_execute_no_params(self, cursor, statement, context=None):
        cursor.execute(statement)

    def is_disconnect(self, e, connection, cursor):
        return False

    def database_create(self, engine, database):
        raise NotImplementedError

    def database_drop(self, engine, database):
        raise NotImplementedError

    def table_create(self, table):
        raise NotImplementedError

    def table_drop(self, table):
        raise NotImplementedError

    def item_create(self, compiled, params):
        raise NotImplementedError

    def item_update(self, compiled, params):
        raise NotImplementedError

    def item_delete(self, compiled, params):
        raise NotImplementedError


class NoSqlApi(Producer):
    apilevel = '2.0'
    threadsafety = 2
    paramstyle = 'pyformat'
    Warning = None
    Error = None
    InterfaceError = None
    DatabaseError = None
    DataError = None
    OperationalError = None
    IntegrityError = None
    InternalError = None
    ProgrammingError = None
    NotSupportedError = None

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
