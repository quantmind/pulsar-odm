import os
import logging
from copy import copy
from contextlib import contextmanager

from sqlalchemy import MetaData, Table, event, inspect
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm.session import Session

from pulsar import ImproperlyConfigured

from .strategy import create_engine


logger = logging.getLogger('pulsar.odm')


class BaseModel(object):

    @declared_attr
    def __tablename__(self):
        return self.__name__.lower()

    @classmethod
    def create_table(cls, name, *columns, **kwargs):
        '''Create a new table wuth the same metadata and info
        '''
        kwargs = table_args(cls, **kwargs)
        table = Table(name, cls.metadata, *columns, **kwargs)
        return table


def table_args(cls, **kwargs):
    args = getattr(cls, '__table_args__', {}).copy()

    for key, value in kwargs.items():
        if key == 'info' and key in args:
            new_value = args['info'].copy()
            new_value.update(value)
            value = new_value
        args[key] = value

    return args


def update_info(cls, info):
    args = getattr(cls, '__table_args__', {})
    if 'info' in args:
        new_info = args['info'].copy()
        if info:
            new_info.update(info)
        return new_info
    else:
        return info


def model_base(bind_label=None, metadata=None, info=None):
    '''Create a base declarative class
    '''
    if metadata is None:
        metadata = MetaData()

    Model = declarative_base(metadata=metadata, cls=BaseModel)
    if bind_label:
        args = getattr(Model, '__table_args__', {})
        if 'info' not in args:
            args['info'] = {}
        args['info']['bind_label'] = bind_label
        Model.__table_args__ = args

    return Model


Model = model_base()


class Mapper:
    '''SQLAlchemy wrapper

    .. attribute:: binds

        Dictionary of labels-engine pairs. The "default" label is always
        present and it is used for tables without `bind_label` in their
        `info` dictionary.
    '''
    def __init__(self, binds):
        # Setup mdoels and engines
        if not binds:
            binds = {}
        elif isinstance(binds, str):
            binds = {'default': binds}
        if binds and 'default' not in binds:
            raise ImproperlyConfigured('default datastore not specified')

        self.metadata = MetaData()
        self._engines = {}
        self._declarative_register = {}
        self.binds = {}
        self.is_green = False

        for name, bind in tuple(binds.items()):
            key = None if name == 'default' else name
            engine = create_engine(bind)
            dialect = engine.dialect
            # Dialect requires Green Pool
            if getattr(dialect, 'is_green', False):
                self.is_green = True
            self._engines[key] = engine

    def __getitem__(self, model):
        return self._declarative_register[model]

    def __getattr__(self, name):
        if name in self._declarative_register:
            return self._declarative_register[name]
        raise AttributeError('No model named "%s"' % name)

    def copy(self, binds):
        return self.__class__(binds)

    def register(self, model):
        metadata = self.metadata
        for table in model.metadata.sorted_tables:
            if table.key not in metadata.tables:
                engine = None
                label = table.info.get('bind_label')
                keys = ('%s.%s' % (label, table.key),
                        label, None) if label else (None,)
                for key in keys:
                    engine = self.get_engine(key)
                    if engine:
                        break
                assert engine
                table.tometadata(self.metadata)
                self.binds[table] = engine

        if (isinstance(model, DeclarativeMeta) and
                hasattr(model, '__table__')):
            table = model.__table__
            self._declarative_register[table.key] = model

    def database_create(self, database, **params):
        '''Create databases for each engine and return a new :class:`.Mapper`.
        '''
        binds = {}
        dbname = database
        for key, engine in self.keys_engines():
            if hasattr(database, '__call__'):
                dbname = database(engine)
            assert dbname, "Cannot create a database, no db name given"
            key = key if key else 'default'
            binds[key] = self._database_create(engine, dbname)
        return self.copy(binds)

    def database_all(self):
        '''Return a dictionary mapping engines with databases
        '''
        all = {}
        for engine in self.engines():
            all[engine] = self._database_all(engine)
        return all

    def database_drop(self, database=None, **params):
        dbname = database
        for engine in self.engines():
            if hasattr(database, '__call__'):
                dbname = database(engine)
            assert dbname, "Cannot drop database, no db name given"
            self._database_drop(engine, dbname)

    def tables(self):
        tables = []
        for engine in self.engines():
            tbs = engine.table_names()
            if tbs:
                tables.append((str(engine.url), tbs))
        return tables

    def table_create(self, remove_existing=False):
        """Creates all tables.
        """
        for engine in self.engines():
            tables = self._get_tables(engine)
            if not remove_existing:
                self.metadata.create_all(engine, tables=tables)
            else:
                pass

    def table_drop(self):
        """Drops all tables.
        """
        for engine in self.engines():
            self.metadata.drop_all(engine, tables=self._get_tables(engine))

    def reflect(self, bind='__all__'):
        """Reflects tables from the database.
        """
        self._execute_for_all_tables(bind, 'reflect', skip_tables=True)

    @contextmanager
    def begin(self, close=True, expire_on_commit=False, **options):
        """Provide a transactional scope around a series of operations.

        By default, ``expire_on_commit`` is set to False so that instances
        can be used outside the session.
        """
        session = self.session(expire_on_commit=expire_on_commit, **options)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            if close:
                session.close()

    def session(self, **options):
        options['binds'] = self.binds
        return OdmSession(self, **options)

    def get_engine(self, key=None):
        '''Get an engine by key
        '''
        if key in self._engines:
            return self._engines[key]

    def engines(self):
        return self._engines.values()

    def keys_engines(self):
        return self._engines.items()

    def close(self):
        for engine in self.engines():
            engine.dispose()

    # INTERNALS
    def _get_tables(self, engine):
        tables = []
        for table, eng in self.binds.items():
            if eng == engine:
                tables.append(table)
        return tables

    def _database_all(self, engine):
        all = self._get_callable(engine, 'database_all')
        return all(engine)

    def _database_create(self, engine, database):
        '''Create a new database and return a new url representing
        a connection to the new database
        '''
        logger.info('Creating database "%s" in "%s"', database, engine)
        create = self._get_callable(engine, 'database_create')
        create(engine, database)
        url = copy(engine.url)
        url.database = database
        return str(url)

    def _database_drop(self, engine, database):
        logger.info('dropping database "%s" from "%s"', database, engine)
        drop = self._get_callable(engine, 'database_drop')
        drop(engine, database)

    def _get_callable(self, engine, method_name):
        dialect = engine.dialect
        if hasattr(dialect, method_name):
            return getattr(dialect, method_name)
        else:
            scripts = engine_scripts[method_name]
            if hasattr(scripts, dialect.name):
                return getattr(scripts, dialect.name)
            else:
                return scripts.default


class CreateDatabase:

    def sqlite(self, engine, database):
        pass

    def default(self, engine, database):
        conn = engine.connect()
        # the connection will still be inside a transaction,
        # so we have to end the open transaction with a commit
        conn.execute("commit")
        conn.execute('create database %s' % database)
        conn.close()


class DropDatabase:

    def sqlite(self, engine, database):
        try:
            os.remove(database)
        except FileNotFoundError:
            pass

    def default(self, engine, database):
        conn = engine.connect()
        conn.execute("commit")
        conn.execute('drop database %s' % database)
        conn.close()


class AllDatabase:

    def sqlite(self, engine):
        database = engine.url.database
        if os.path.isfile(database):
            return [database]
        else:
            return []

    def default(self, engine):
        insp = inspect(engine)
        return insp.get_schema_names()


engine_scripts = {'database_create': CreateDatabase(),
                  'database_drop': DropDatabase(),
                  'database_all': AllDatabase()}


class OdmSession(Session):
    """The sql alchemy session that lux uses.

    It extends the default session system with bind selection and
    modification tracking.
    """

    def __init__(self, mapper, **options):
        #: The application that this session belongs to.
        self.mapper = mapper
        self.register()
        super().__init__(**options)

    def register(self):
        if not hasattr(self, '_model_changes'):
            self._model_changes = {}

        event.listen(self, 'before_flush', self.record_ops)
        event.listen(self, 'before_commit', self.record_ops)
        event.listen(self, 'before_commit', self.before_commit)
        event.listen(self, 'after_commit', self.after_commit)
        event.listen(self, 'after_rollback', self.after_rollback)

    @classmethod
    def signal(cls, session, changes, event):
        '''Signal changes on session
        '''
        pass

    @classmethod
    def record_ops(cls, session, flush_context=None, instances=None):
        try:
            d = session._model_changes
        except AttributeError:
            return

        for targets, operation in ((session.new, 'insert'),
                                   (session.dirty, 'update'),
                                   (session.deleted, 'delete')):
            for target in targets:
                state = inspect(target)
                key = state.identity_key if state.has_identity else id(target)
                d[key] = (target, operation)

    @classmethod
    def before_commit(cls, session):
        try:
            d = session._model_changes
        except AttributeError:
            return

        if d:
            cls.signal(session, d, 'on_before_commit')

    @classmethod
    def after_commit(cls, session):
        try:
            d = session._model_changes
        except AttributeError:
            return

        if d:
            cls.signal(session, d, 'on_after_commit')
            d.clear()

    @classmethod
    def after_rollback(cls, session):
        try:
            d = session._model_changes
        except AttributeError:
            return

        d.clear()
