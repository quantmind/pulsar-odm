import unittest
import string
import inspect
from functools import wraps
from copy import copy
from contextlib import contextmanager
from datetime import datetime

import odm

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import sessionmaker

from pulsar.apps.test import TestPlugin
from pulsar.utils.string import random_string
from pulsar.apps.greenio import GreenPool


class PostgreSql(TestPlugin):
    name = 'postgresql'
    meta = "CONNECTION_STRING"
    default = 'postgresql+async://odm:odmtest@127.0.0.1:5432/odmtests'
    desc = 'Default connection string for the PostgreSql server'


class Task(odm.Model):
    id = Column(Integer, primary_key=True)
    subject = Column(String(250))
    done = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)


def randomname(prefix):
    name = random_string(min_len=8, max_len=8, characters=string.ascii_letters)
    return ('%s%s' % (prefix, name)).lower()


def green(method):
    if inspect.isclass(method):
        cls = method
        for name in dir(cls):
            if name.startswith('test'):
                method = getattr(cls, name)
                setattr(cls, name, green(method))

        return cls
    else:
        @wraps(method)
        def _(self):
            return self.pool.submit(method, self)

        return _


class TestCase(unittest.TestCase):
    prefixdb = 'odmtest_'
    models = (Task,)
    # Tuple of SqlAlchemy models to register
    init_engine = None

    @classmethod
    def setUpClass(cls):
        # Create the application
        cls.dbs = {}
        cls.pool = GreenPool()
        odm.logger.info('Create test databases')
        cls.dbname = randomname(cls.prefixdb)
        cls.init_engine = yield from cls.pool.submit(cls.setupdb, cls.dbname)
        url = copy(cls.init_engine.url)
        url.database = cls.dbname
        cls.engine = create_engine(str(url))
        yield from cls.pool.submit(cls.table_create)
        cls.session = sessionmaker(bind=cls.engine)

    @classmethod
    def table_create(cls):
        cls.metadata = MetaData()
        metadata = cls.metadata
        for value in cls.models:
            for table in value.metadata.sorted_tables:
                if table.key not in metadata.tables:
                    table.tometadata(metadata)
        odm.logger.info('Create test tables')
        metadata.create_all(cls.engine)

    @classmethod
    def tearDownClass(cls):
        # Create the application
        if cls.engine:
            return cls.pool.submit(cls.dropdb, cls.dbname)

    @classmethod
    def setupdb(cls, dbname):
        raise NotImplementedError

    @classmethod
    def dropdb(cls, dbname):
        cls.engine.dispose()
        conn = cls.init_engine.connect()
        conn.execute("commit")
        conn.execute('drop database %s' % dbname)
        conn.close()

    @classmethod
    @contextmanager
    def begin(cls, close=True, expire_on_commit=False, **options):
        """Provide a transactional scope around a series of operations.

        By default, ``expire_on_commit`` is set to False so that instances
        can be used outside the session.
        """
        session = cls.session(expire_on_commit=expire_on_commit, **options)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            if close:
                session.close()
