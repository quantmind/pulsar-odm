import unittest
import string
import inspect
from contextlib import contextmanager
from datetime import datetime

import odm

from sqlalchemy import Column, Integer, String, Boolean, DateTime
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
        for name, method in inspect.getmembers(cls,
                                               predicate=inspect.ismethod):
            if name.startswith('test'):
                setattr(cls, name, green(method))

        return cls
    else:
        @wraps(method)
        def _(self):
            return self.pool.submit(method, self)

        return _


class TestCase(unittest.TestCase):
    prefixdb = 'odmtest_'
    engine = None

    @classmethod
    def setUpClass(cls):
        # Create the application
        cls.dbs = {}
        cls.pool = GreenPool()
        odm.logger.info('Create test databases')
        cls.dbname = randomname(cls.prefixdb)
        cls.engine = yield from cls.pool.submit(cls.setupdb, cls.dbname)
        cls.session = sessionmaker(bind=cls.engine)
        odm.logger.info('Create test tables')
        # cls.table_create()

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
