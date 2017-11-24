import unittest
from inspect import isclass, getmodule

from sqlalchemy import Column, Integer, String, Table

from odm import mapper

from tests.base import Employee, Engineer


Model = mapper.model_base('testutil')


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
        Model = mapper.model_base('foooo')
        self.assertTrue(isclass(Model))
        self.assertTrue(Model.__table_args__)
        self.assertEqual(Model.__table_args__[0]['info'],
                         {'bind_label': 'foooo'})

    def test_table_args(self):
        self.assertTrue(Engineer.__table_args__)
        self.assertTrue(Engineer.__table_args__[0]['info'])

    def test_table(self):
        mp = mapper.Mapper('sqlite:///')
        # Model = mapper.model_base('custom')
        table = mp.create_table('user', Column('name', String(80)))
        self.assertIsInstance(table, Table)
        # self.assertEqual(table.info['bind_label'], 'custom')

    def test_model_sandbox(self):
        mp = mapper.Mapper('sqlite:///')
        model = mp.register(Foo)
        self.assertEqual(model, mp.foo)
        self.assertEqual(mp.metadata, model.metadata)
        self.assertTrue(issubclass(model, Foo))

    def test_register_polimorfic(self):
        mp = mapper.Mapper('sqlite:///')
        mp.register(Employee)
        mp.register(Engineer)

    def test_getitem(self):
        mp = mapper.Mapper('sqlite:///')
        model = mp.register(Employee)
        self.assertEqual(mp['employee'], model)
        self.assertEqual(mp.employee, model)
        self.assertRaises(AttributeError, lambda: mp.foo)

    def test_register_module(self):
        mp = mapper.Mapper('sqlite:///')
        mp.register_module(getmodule(self))
        self.assertTrue(mp.foo)
        self.assertRaises(AttributeError, lambda: mp.bla)
        self.assertEqual(len(mp.metadata.tables), 2)
        bla = mp.metadata.tables['bla']
        self.assertTrue(bla.key, 'bla')

    def test_no_binds(self):
        self.assertRaises(mapper.ImproperlyConfigured, mapper.Mapper, None)

    def test_copy_modules(self):
        module = getmodule(self)
        models = mapper.get_models(module)
        mapper.copy_models(module, __name__)
        new_models = mapper.get_models(module)
        self.assertIsInstance(new_models.pop('bla'), Table)
        self.assertTrue(new_models)
        self.assertNotEqual(id(models), id(new_models))
