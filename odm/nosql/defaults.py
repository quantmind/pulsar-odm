import sqlalchemy
from sqlalchemy.engine import Connectable, ResultProxy
from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.sql import compiler, crud
from sqlalchemy import util

from pulsar import Producer

CREATE = 'CREATE'
UPDATE = 'UPDATE'
DELETE = 'DELETE'
CREATE_TABLE = 'CREATE_TABLE'
DROP_TABLE = 'DROP_TABLE'
TABLE_STATEMENTS = frozenset((CREATE_TABLE, DROP_TABLE))


class NoSqlConnection:
    '''Abstract class for Connections
    '''
    def rollback(self):
        pass

    def commit(self):
        pass

    def cursor(self):
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


class NoSqlResultProxy(ResultProxy):
    pass


class NoSqlExecutionContext(DefaultExecutionContext):

    def get_result_proxy(self):
        return NoSqlResultProxy(self)


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
