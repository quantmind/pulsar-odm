import unittest
from inspect import isclass

from sqlalchemy import Column, Integer, String

import odm


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
        Model = odm.model_base('foooo')

        class Base(Model):
            id = Column(Integer, primary_key=True)
            name = Column(String(80))

        args = Base.__table_args__.copy()
        args['extend_existing'] = True

        class Base(Base):
            __table_args__ = args
            age = Column(Integer)

        self.assertTrue(Base.__table_args__)
        self.assertTrue(Base.__table_args__['extend_existing'])
        self.assertTrue(Base.__table_args__['info'])

    def test_table(self):
        Model = odm.model_base('custom')
        table = Model.create_table('User', Column('name', String(80)))
        self.assertEqual(table.info['bind_label'], 'custom')
