import unittest
import string
import inspect
from enum import Enum
from uuid import uuid4
from functools import wraps
from datetime import datetime

from odm import mapper
from odm.types import JSONType, UUIDType, ChoiceType

from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, Boolean, DateTime, Integer, ForeignKey

from pulsar.utils.string import random_string
from pulsar.apps.greenio import GreenPool


Model = mapper.model_base('foooo')


class TaskType(Enum):
    work = 1
    personal = 2
    social = 3


class PersonalTasks(Model):
    id = Column(UUIDType, primary_key=True)
    subject = Column(String(250))
    done = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)

    __create_sql__ = """
    create or replace view {0[name]} as (
    with
        -- QUERY
        _tasks as (
            select *
            from task
            where type = 2
        ),

        _personal_view as (
          select
            t.id,
            t.subject,
            t.done,
            t.created
          from _tasks t
        )

        select * from _personal_view ps
    )
    """

    __drop_sql__ = """
    drop view {0[name]}
    """


class Employee(Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(80))
    type = Column(String(50))
    sex = Column(ChoiceType({'female': 'female', 'male': 'male'}))

    @mapper.declared_attr
    def __mapper_args__(cls):
        name = cls.__name__.lower()
        if cls.__name__ == 'Employee':
            return {
                'polymorphic_identity': name,
                'polymorphic_on': cls.type
            }
        else:
            return {
                'polymorphic_identity': name
            }


class Engineer(Employee):
    engineer_name = Column(String(30))

    @mapper.declared_attr
    def id(self):
        return Column(Integer, ForeignKey('employee.id'), primary_key=True)


class Task(Model):
    id = Column(UUIDType, primary_key=True)
    subject = Column(String(250))
    done = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)
    info = Column(JSONType)
    info2 = Column(JSONType(binary=False))
    type = Column(ChoiceType(TaskType, impl=Integer),
                  default=TaskType.work)

    @mapper.declared_attr
    def employee_id(cls):
        return Column(Integer, ForeignKey('employee.id'))

    @mapper.declared_attr
    def employee(cls):
        return relationship('Employee', backref='tasks')


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
    # Tuple of SqlAlchemy models to register
    mapper = None

    @classmethod
    async def setUpClass(cls):
        # Create the application
        cls.dbs = {}
        cls.dbname = randomname(cls.prefixdb)
        cls.init_mapper = mapper.Mapper(cls.url())
        cls.green_pool = GreenPool()
        cls.mapper = await cls.green_pool.submit(
            cls.init_mapper.database_create,
            cls.dbname
        )
        cls.mapper.register_module(__name__)
        await cls.green_pool.submit(cls.mapper.table_create)

    @classmethod
    async def tearDownClass(cls):
        # Create the application
        if cls.mapper:
            pool = cls.green_pool
            await pool.submit(cls.mapper.close)
            await pool.submit(cls.init_mapper.database_drop, cls.dbname)

    @classmethod
    def url(cls):
        '''Url for database to test
        '''
        raise NotImplementedError


class MapperMixin:

    def test_mapper(self):
        mapper = self.mapper
        self.assertTrue(mapper.binds)

    def test_databases(self):
        dbs = self.mapper.database_all()
        self.assertIsInstance(dbs, dict)

    def test_tables(self):
        tables = self.mapper.tables()
        self.assertTrue(tables)
        self.assertEqual(len(tables[0][1]), 3)

    def test_database_drop_fail(self):
        self.assertRaises(AssertionError,
                          self.mapper.database_drop,
                          lambda e: None)

    def test_create_task(self):
        with self.mapper.begin() as session:
            task = self.mapper.task(id=uuid4(),
                                    subject='simple task',
                                    type=TaskType.personal)
            session.add(task)
        self.assertTrue(task.id)
        self.assertEqual(task.type, TaskType.personal)

        with self.mapper.begin() as session:
            task = session.query(self.mapper.task).get(task.id)
            self.assertEqual(task.type, TaskType.personal)

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

    def test_task_employee(self):
        mapper = self.mapper

        with mapper.begin() as session:
            user = mapper.employee(name='pippo', sex='male')
            session.add(user)

        with mapper.begin() as session:
            task = mapper.task(id=uuid4(),
                               employee_id=user.id,
                               subject='simple task to update')
            session.add(task)

        with mapper.begin() as session:
            user = session.query(mapper.employee).get(user.id)
            tasks = user.tasks
            self.assertTrue(tasks)
            self.assertEqual(user.sex, 'male')

    def test_view(self):
        mapper = self.mapper

        with mapper.begin() as session:
            session.add(mapper.task(id=uuid4(),
                                    subject='simple task 1',
                                    type=TaskType.personal))
            session.add(mapper.task(id=uuid4(),
                                    subject='simple task 2',
                                    type=TaskType.personal))
            session.add(mapper.task(id=uuid4(),
                                    subject='simple task 3',
                                    type=TaskType.work))

        with mapper.begin() as session:
            ptasks = session.query(mapper.personaltasks).all()
            self.assertTrue(len(ptasks) >= 2)

    def test_database_exist(self):
        binds = self.mapper.database_exist()
        self.assertTrue(binds)
        self.assertTrue(binds['default'])
