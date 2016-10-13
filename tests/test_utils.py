import unittest
from inspect import isclass, getmodule

from sqlalchemy import Column, Integer, String, Table

import odm

from tests.base import Employee, Engineer


Model = odm.model_base('testutil')


class Foo(Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(100))


table = Model.create_table(
    'bla',
    Column('id', Integer, primary_key=True),
    Column('name', String),
)


class TestUtils(unittest.TestCase):

    def test_model_base(self):
        Model = odm.model_base('foooo')
        self.assertTrue(isclass(Model))
        self.assertTrue(Model.__table_args__)
        self.assertEqual(Model.__table_args__[0]['info'],
                         {'bind_label': 'foooo'})

    def test_table_args(self):
        self.assertTrue(Engineer.__table_args__)
        self.assertTrue(Engineer.__table_args__[0]['info'])

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

    def test_getitem(self):
        mapper = odm.Mapper('sqlite:///')
        model = mapper.register(Employee)
        self.assertEqual(mapper['employee'], model)
        self.assertEqual(mapper.employee, model)
        self.assertRaises(AttributeError, lambda: mapper.foo)

    def test_register_module(self):
        mapper = odm.Mapper('sqlite:///')
        mapper.register_module(getmodule(self))
        self.assertTrue(mapper.foo)
        self.assertRaises(AttributeError, lambda: mapper.bla)
        self.assertEqual(len(mapper.metadata.tables), 2)
        bla = mapper.metadata.tables['bla']
        self.assertTrue(bla.key, 'bla')

    def test_no_binds(self):
        self.assertRaises(odm.ImproperlyConfigured, odm.Mapper, None)

    def test_copy_modules(self):
        module = getmodule(self)
        models = odm.get_models(module)
        odm.copy_models(module, __name__)
        new_models = odm.get_models(module)
        self.assertIsInstance(new_models.pop('bla'), Table)
        self.assertTrue(new_models)
        self.assertNotEqual(id(models), id(new_models))
