import unittest
from collections import Mapping

import odm
from odm.model import ModelType

from .data import User


class TestOdm(unittest.TestCase):

    def test_model(self):
        model = odm.Model()
        self.assertFalse(model)
        self.assertIsInstance(model, dict)
        self.assertEqual(len(model), 0)
        self.assertEqual(type(model.__class__), ModelType)
        model['bla'] = 'ciao'
        self.assertEqual(len(model), 1)
        meta = model._meta
        self.assertEqual(meta, odm.Model._meta)
        self.assertTrue(meta.abstract)

    def test_user(self):
        user = User(username='pippo', bla='foo')
        self.assertTrue(user)
        self.assertEqual(len(user), 2)
        self.assertEqual(user.username, 'pippo')
        self.assertEqual(user['username'], 'pippo')
        self.assertEqual(user.bla, 'foo')
        self.assertEqual(user['bla'], 'foo')
        meta = user._meta
        self.assertFalse(meta.abstract)

    def test_errors(self):
        self.assertRaises(odm.FieldError, odm.create_model, 'Model1',
                          _bla=odm.CharField())
        self.assertRaises(
            odm.FieldError, odm.create_model, 'Model1',
            foo=odm.IntegerField(primary_key=True),
            bla=odm.CharField(primary_key=True))
