import weakref

from sqlalchemy.engine import Dialect
from sqlalchemy import util, types

from .defaults import NoSqlCompiler, NoSqlExecutionContext, NoSqlCompiler
from .pool import GreenPool


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
    execution_ctx_cls = NoSqlExecutionContext
    ddl_compiler = NoSqlCompiler
    statement_compiler = NoSqlCompiler
    dbapi_type_map = {}
    case_sensitive = False
    description_encoding = None
    requires_name_normalize = False
    colspecs = {}

    def __init__(self, dbapi=None, **kw):
        self.dbapi = dbapi
        self.paramstyle = self.dbapi.paramstyle

    @classmethod
    def get_pool_class(cls, url):
        return getattr(cls, 'poolclass', GreenPool)

    @property
    def dialect_description(self):
        return self.name + "+" + self.driver

    def type_descriptor(self, typeobj):
        return types.adapt_type(typeobj, self.colspecs)

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

    @util.memoized_property
    def _type_memos(self):
        return weakref.WeakKeyDictionary()
