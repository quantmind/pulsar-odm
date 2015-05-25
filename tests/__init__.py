import unittest
import string
import inspect
from functools import wraps
from datetime import datetime

import odm

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
    mapper = None

    @classmethod
    def setUpClass(cls):
        # Create the application
        cls.dbs = {}
        cls.pool = GreenPool()
        cls.dbname = randomname(cls.prefixdb)
        cls.init_mapper = odm.Mapper(cls.url())
        cls.mapper = yield from cls.pool.submit(
            cls.init_mapper.database_create, cls.dbname)
        for model in cls.models:
            cls.mapper.register(model)
        yield from cls.pool.submit(cls.mapper.table_create)

    @classmethod
    def tearDownClass(cls):
        # Create the application
        if cls.mapper:
            yield from cls.pool.submit(cls.mapper.close)
            yield from cls.pool.submit(cls.init_mapper.database_drop,
                                       cls.dbname)

    @classmethod
    def url(cls):
        '''Url for database to test
        '''
        raise NotImplementedError


class MapperMixin:

    def test_mapper(self):
        mapper = self.mapper
        self.assertTrue(mapper.binds)
