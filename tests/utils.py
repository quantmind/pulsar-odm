import unittest
from inspect import isclass

from sqlalchemy import Column, Integer, String, Table

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

    def test_table_args(self):
        self.assertTrue(Engineer.__table_args__)
        self.assertTrue(Engineer.__table_args__['info'])

    def test_table(self):
        mapper = odm.Mapper('sqlite:///')
        # Model = odm.model_base('custom')
        table = mapper.create_table('user', Column('name', String(80)))
        self.assertIsInstance(table, Table)
        # self.assertEqual(table.info['bind_label'], 'custom')

    def test_model_sandbox(self):
        mapper = odm.Mapper('sqlite:///')
        model = mapper.register(Foo)
        self.assertEqual(model, mapper.foo)
        self.assertEqual(mapper.metadata, model.metadata)
        self.assertTrue(issubclass(model, Foo))

    def test_register_polimorfic(self):
        mapper = odm.Mapper('sqlite:///')
        mapper.register(Employee)
        mapper.register(Engineer)
