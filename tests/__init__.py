import unittest
import string
import inspect
import asyncio

from uuid import uuid4
from functools import wraps
from datetime import datetime

import odm
from odm.types import JSONType, UUIDType

from sqlalchemy import Column, String, Boolean, DateTime, Integer, ForeignKey

from pulsar.utils.string import random_string
from pulsar.apps.greenio import GreenPool


Model = odm.model_base('foooo')


class Task(odm.Model):
    id = Column(UUIDType, primary_key=True)
    subject = Column(String(250))
    done = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)
    info = Column(JSONType)
    info2 = Column(JSONType(binary=False))


class Employee(Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(80))
    type = Column(String(50))

    __mapper_args__ = {
        'polymorphic_identity': 'employee',
        'polymorphic_on': type
    }


class Engineer(Employee):
    id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
    engineer_name = Column(String(30))

    __mapper_args__ = {
        'polymorphic_identity': 'engineer'
    }


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
            return self.green_pool.submit(method, self)

        return _


class TestCase(unittest.TestCase):
    prefixdb = 'odmtest_'
    models = (Task, Employee, Engineer)
    # Tuple of SqlAlchemy models to register
    mapper = None

    @classmethod
    @asyncio.coroutine
    def setUpClass(cls):
        # Create the application
        cls.dbs = {}
        cls.dbname = randomname(cls.prefixdb)
        cls.init_mapper = odm.Mapper(cls.url())
        cls.green_pool = GreenPool()
        cls.mapper = yield from cls.green_pool.submit(
            cls.init_mapper.database_create, cls.dbname)
        for model in cls.models:
            cls.mapper.register(model)
        yield from cls.green_pool.submit(cls.mapper.table_create)

    @classmethod
    @asyncio.coroutine
    def tearDownClass(cls):
        # Create the application
        if cls.mapper:
            pool = cls.green_pool
            yield from pool.submit(cls.mapper.close)
            yield from pool.submit(cls.init_mapper.database_drop,
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

    def test_create_task(self):
        with self.mapper.begin() as session:
            task = self.mapper.task(id=uuid4(),
                                    subject='simple task')
            session.add(task)
        self.assertTrue(task.id)

    def test_update_task(self):
        with self.mapper.begin() as session:
            task = self.mapper.task(id=uuid4(),
                                    subject='simple task to update')
            task.info = dict(extra='extra info')
            session.add(task)
        self.assertTrue(task.id)
        self.assertFalse(task.done)
        self.assertEqual(task.info['extra'], 'extra info')
        with self.mapper.begin() as session:
            task.done = True
            session.add(task)

        with self.mapper.begin() as session:
            task = session.query(self.mapper.task).get(task.id)

        self.assertTrue(task.done)
        self.assertEqual(task.info['extra'], 'extra info')
