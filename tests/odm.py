import unittest
import json
from datetime import datetime
from collections import Mapping

import odm
from odm.model import ModelType

from .data.models import User, Session


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

    def test_dummy_store(self):
        mapper = odm.Mapper('dummy://')
        self.assertEqual(mapper.default_store.name, 'dummy')
        self.assertTrue(repr(mapper))
        self.assertRaises(AttributeError, lambda: mapper.user)

    def test_register_applications(self):
        mapper = odm.Mapper('dummy://')
        models = mapper.register_applications(['tests.data'])
        self.assertEqual(len(models), 3)
        self.assertEqual(len(mapper), 3)

    def test_unregister_models(self):
        mapper = odm.Mapper('dummy://')
        models = mapper.register_applications('tests.data')
        self.assertEqual(len(models), 3)
        self.assertEqual(len(mapper), 3)
        models = mapper.unregister()
        self.assertEqual(len(models), 3)
        self.assertEqual(len(mapper), 0)
        result = yield from mapper.flush()
        self.assertFalse(result)

    def test_query(self):
        mapper = odm.Mapper('dummy://')
        mapper.register_applications('tests.data')
        query = mapper.user.query()
        self.assertEqual(query._mapper, mapper)
        self.assertEqual(query._loop, None)

    def test_to_dict(self):
        mapper = odm.Mapper('dummy://')
        mapper.register_applications(['tests.data'])
        user = mapper.user(username='pippo', bla='foo', password='bjhvbdfjv')
        self.assertTrue(user)
        data = user.todict()
        self.assertEqual(len(data), 2)
        data, _ = mapper.user._store.model_data(user)
        self.assertTrue(len(data) > 2)
        data = user.todict()
        self.assertEqual(len(data), 5)
        json.dumps(data)
