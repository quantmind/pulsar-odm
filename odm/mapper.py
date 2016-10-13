import logging
import sys
from copy import copy
from inspect import getmodule
from contextlib import contextmanager
from collections import OrderedDict

from sqlalchemy import MetaData, Table, event, inspect
from sqlalchemy.ext.declarative.api import (declarative_base, declared_attr,
                                            _as_declarative, _add_attribute)
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import object_session
from sqlalchemy.schema import DDL

from pulsar import ImproperlyConfigured

from .strategy import create_engine
from .utils import database_operation


logger = logging.getLogger('pulsar.odm')


class OdmMeta(type):

    def __new__(cls, name, bases, attrs):
        abstract = attrs.pop('__odm_abstract__', False)
        klass = super().__new__(cls, name, bases, attrs)
        if not abstract and not isinstance(klass, DeclarativeMeta):
            module = getmodule(klass)
            models = getattr(module, '__odm_models__', None)
            if models is None:
                models = OrderedDict()
                module.__odm_models__ = models
            name = klass.__name__.lower()
            models[name] = klass
        return klass


class DeclarativeMeta(OdmMeta):

    def __init__(cls, classname, bases, dict_):
        if '_decl_class_registry' not in cls.__dict__:
            _as_declarative(cls, classname, cls.__dict__)
        type.__init__(cls, classname, bases, dict_)

    def __setattr__(cls, key, value):
        _add_attribute(cls, key, value)


class BaseModel(metaclass=OdmMeta):
    __odm_abstract__ = True

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @classmethod
    def create_table(cls, name, *columns, **kwargs):
        targs = table_args(cls, **kwargs)
        args, kwargs = targs[:-1], targs[-1]
        table = Table(name, MetaData(), *columns, *args, **kwargs)
        return table


def table_args(*args, **kwargs):
    targs = ()
    tkwargs = {}

    if args:
        if hasattr(args[0], '__table_args__'):
            targs = args[0].__table_args__
            targs, tkwargs = targs[:-1], targs[-1].copy()
            args = args[1:]

        targs += args

    for key, value in kwargs.items():
        if isinstance(value, dict) and key in tkwargs:
            new_value = tkwargs[key].copy()
            new_value.update(value)
            value = new_value
        tkwargs[key] = value

    return targs + (tkwargs,)


def model_base(bind_label=None, info=None):
    """Create a base declarative class
    """
    Model = type('Model', (BaseModel,), {'__odm_abstract__': True})
    info = {}
    Model.__table_args__ = table_args(info=info)
    if bind_label:
        info['bind_label'] = bind_label
    return Model


def module_tables(module):
    for name, table in vars(module).items():
        if isinstance(table, Table):
            yield table


def copy_models(module_from, module_to):
    """Copy models from one module to another
    :param module_from:
    :param module_to:
    :return:
    """
    module_from = get_module(module_from)
    module_to = get_module(module_to)
    models = get_models(module_from)
    if models:
        models = models.copy()
        models.update(((t.key, t) for t in module_tables(module_from)))
        module_to.__odm_models__ = models
        return models


def move_models(module_from, module_to):
    module_from = get_module(module_from)
    if copy_models(module_from, module_to):
        del module_from.__odm_models__


def get_module(module_or_name):
    if isinstance(module_or_name, str):
        return sys.modules[module_or_name]
    else:
        return getmodule(module_or_name)


def get_models(module):
    """Get models from a module
    :param module:
    :return:
    """
    return getattr(get_module(module), '__odm_models__', None)


Model = model_base()


class Mapper:
    """SQLAlchemy wrapper

    .. attribute:: binds

        Dictionary of labels-engine pairs. The "default" label is always
        present and it is used for tables without `bind_label` in their
        `info` dictionary.
    """
    def __init__(self, binds):
        # Setup mdoels and engines
        if not binds:
            binds = {}
        elif isinstance(binds, str):
            binds = {'default': binds}
        if 'default' not in binds:
            raise ImproperlyConfigured('default datastore not specified')

        self._engines = {}
        self._declarative_register = {}
        self._bases = {}
        self._base_declarative = declarative_base(name='OdmBase',
                                                  metaclass=DeclarativeMeta)
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

    @property
    def metadata(self):
        """Returns the :class:`~sqlalchemy.Metadata` for this mapper
        """
        return self._base_declarative.metadata

    def copy(self, binds):
        return self.__class__(binds)

    def register(self, model):
        """Register a model or a table with this mapper

        :param model: a table or a :class:`.BaseModel` class
        :return: a Model class or a table
        """
        metadata = self.metadata
        if not isinstance(model, Table):
            model_name = self._create_model(model)
            if not model_name:
                return
            model, name = model_name
            table = model.__table__
            self._declarative_register[name] = model

            if name in self._bases:
                for model in self._bases.pop(name):
                    self.register(model)
        else:
            table = model.tometadata(metadata)
            model = table

        # Register engine
        engine = None
        label = table.info.get('bind_label')
        keys = ('%s.%s' % (label, table.key),
                label, None) if label else (None,)
        #
        # Find the engine for this table
        for key in keys:
            engine = self.get_engine(key)
            if engine:
                break
        assert engine
        self.binds[table] = engine

        return model

    def register_module(self, module, exclude=None):
        module = get_module(module)
        models = get_models(module)
        exclude = set(exclude or ())
        if models:
            for name, model in models.items():
                if name in exclude:
                    continue
                self.register(model)
        for table in module_tables(module):
            if table.key not in exclude:
                self.register(table)

    def create_table(self, name, *columns, **kwargs):
        """Create a new table with the same metadata and info
        """
        targs = table_args(**kwargs)
        args, kwargs = targs[:-1], targs[-1]
        return Table(name, self.metadata, *columns, *args, **kwargs)

    def database_create(self, database, **params):
        """Create databases for each engine and return a new :class:`.Mapper`.
        """
        binds = {}
        dbname = database
        for key, engine in self.keys_engines():
            if hasattr(database, '__call__'):
                dbname = database(engine)
            assert dbname, "Cannot create a database, no db name given"
            key = key if key else 'default'
            binds[key] = self._database_create(engine, dbname)
        return self.copy(binds)

    def database_exist(self):
        """Create databases for each engine and return a new :class:`.Mapper`.
        """
        binds = {}
        for key, engine in self.keys_engines():
            key = key if key else 'default'
            binds[key] = self._database_exist(engine)
        return binds

    def database_all(self):
        """Return a dictionary mapping engines with databases
        """
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
            tables = self._get_tables(engine, create_drop=True)
            logger.info('Create all tables for %s', engine)
            self.metadata.create_all(engine, tables=tables)

    def table_drop(self):
        """Drops all tables.
        """
        for engine in self.engines():
            tables = self._get_tables(engine, create_drop=True)
            logger.info('Drop all tables for %s', engine)
            self.metadata.drop_all(engine, tables=tables)

    @contextmanager
    def begin(self, close=True, expire_on_commit=False, session=None,
              commit=False, **options):
        """Provide a transactional scope around a series of operations.

        By default, ``expire_on_commit`` is set to False so that instances
        can be used outside the session.
        """
        if not session:
            commit = True
            session = self.session(expire_on_commit=expire_on_commit,
                                   **options)
        else:
            close = False
        try:
            yield session
            if commit:
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

    def session_from_object(self, *objs):
        for obj in objs:
            session = object_session(obj)
            if session is not None:
                return session

    def dialect(self, key):
        """Dialect object for a model/table name
        """
        return self.binds[self[key].__table__].dialect

    def get_engine(self, key=None):
        """Get an engine by key
        """
        if key in self._engines:
            return self._engines[key]

    def engines(self):
        """Iterator over all engines
        """
        return self._engines.values()

    def keys_engines(self):
        return self._engines.items()

    def close(self):
        for engine in self.engines():
            engine.dispose()

    # INTERNALS
    def _create_model(self, model):
        model_name = model.__name__
        meta = type(self._base_declarative)
        if isinstance(model, meta):
            raise ImproperlyConfigured('Cannot register declarative classes '
                                       'only mixins allowed')
        base = getattr(model, '__inherit_from__', None)
        if base:
            if base not in self._declarative_register:
                models = self._bases.get(base)
                if not models:
                    self._bases[base] = models = []
                models.append(model)
                return
            else:
                base = self._declarative_register[base]
        else:
            base = self._base_declarative

        #
        # Create SqlAlchemy Model
        model = meta(model_name, (model, base), {})
        create = getattr(model, '__create_sql__', None)
        name = model_name.lower()
        if create:
            event.listen(self.metadata,
                         'after_create',
                         DDL(create.format({'name': name})))
            drop = getattr(model, '__drop_sql__', None)
            if not drop:
                logger.warning('Model %s has create statement but not drop. '
                               'To mute this warning add a __drop_sql__ '
                               'statement in the model class', name)
            else:
                event.listen(self.metadata,
                             'before_drop',
                             DDL(drop.format({'name': name})))

        return model, name

    def _get_tables(self, engine, create_drop=False):
        tables = []
        for table, eng in self.binds.items():
            if eng == engine:
                if table.key in self._declarative_register:
                    model = self[table.key]
                    if create_drop and hasattr(model, '__create_sql__'):
                        continue
                tables.append(table)
        return tables

    def _database_all(self, engine):
        return database_operation(engine, 'all')

    def _database_create(self, engine, database):
        """Create a new database and return a new url representing
        a connection to the new database
        """
        logger.info('Creating database "%s" in "%s"', database, engine)
        database_operation(engine, 'create', database)
        url = copy(engine.url)
        url.database = database
        return str(url)

    def _database_drop(self, engine, database):
        logger.info('dropping database "%s" from "%s"', database, engine)
        database_operation(engine, 'drop', database)

    def _database_exist(self, engine):
        return database_operation(engine, 'exists')


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
        """Signal changes on session
        """
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
