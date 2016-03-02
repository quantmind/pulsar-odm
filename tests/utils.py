import unittest
from inspect import isclass

from sqlalchemy import Column, Integer, String

import odm

from . import Employee, Engineer


class Foo(odm.Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(100))


class TestUtils(unittest.TestCase):

    def test_model_base(self):
        Model = odm.model_base('foooo')
        self.assertTrue(isclass(Model))
        self.assertTrue(Model.__table_args__)
        self.assertEqual(Model.__table_args__['info'],
                         {'bind_label': 'foooo'})
        Model2 = odm.model_base('bla')
        self.assertNotEqual(Model.metadata, Model2.metadata)

    def test_table_args(self):
        self.assertTrue(Engineer.__table_args__)
        self.assertTrue(Engineer.__table_args__['info'])

    def test_table(self):
        Model = odm.model_base('custom')
        table = Model.create_table('User', Column('name', String(80)))
        self.assertEqual(table.info['bind_label'], 'custom')

    def test_model_sandbox(self):
        mapper = odm.Mapper('sqlite:///')
        mapper.register(Foo)
        self.assertNotEqual(mapper.metadata, Foo.metadata)
        self.assertTrue(mapper.foo)
        self.assertNotEqual(mapper.foo, Foo)
        self.assertEqual(mapper.metadata, mapper.foo.metadata)
        manager = mapper.foo._sa_class_manager
        self.assertEqual(manager.class_, mapper.foo)
        self.assertEqual(manager.mapper.class_, mapper.foo)
        self.assertEqual(manager.mapper.base_mapper, mapper.foo.__mapper__)

    def test_register_polimorfic(self):
        mapper = odm.Mapper('sqlite:///')
        mapper.register(Employee)
        mapper.register(Engineer)
