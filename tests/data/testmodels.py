import unittest
import string
from datetime import datetime, timedelta

from pulsar.apps.test import random_string

import odm


default_expiry = lambda: datetime.now() + timedelta(days=7)

randomname = lambda : random_string(min_len=8, max_len=8,
                                    characters=string.ascii_letters)


class User(odm.Model):
    username = odm.CharField(unique=True)
    password = odm.CharField(required=False, hidden=True)
    first_name = odm.CharField(required=False, index=True)
    last_name = odm.CharField(required=False, index=True)
    email = odm.CharField(required=False, unique=True)
    is_active = odm.BooleanField(default=True)
    can_login = odm.BooleanField(default=True)
    is_superuser = odm.BooleanField(default=False)


class Session(odm.Model):
    '''A session model with a hash table as data store.'''
    expiry = odm.DateTimeField(default=default_expiry)
    user = odm.ForeignKey(User)


class Blog(odm.Model):
    published = odm.DateField()
    title = odm.CharField()
    body = odm.CharField()


class OdmTests(unittest.TestCase):
    models = (User, Session, Blog)

    @classmethod
    def create_mapper(cls, **kw):
        '''Create a mapper for models'''
        mapper = odm.Mapper(cls.store)
        for model in cls.models:
            mapper.register(model)
        return mapper

    @classmethod
    def setUpClass(cls):
        cls.dbname = randomname()
        cls.store = cls.create_store()
        yield from cls.store.database_create(cls.dbname)
        cls.mapper = cls.create_mapper()
        #
        # Create the tables
        yield from cls.mapper.table_create()

    @classmethod
    def tearDownClass(cls):
        yield from cls.store.database_drop()

    def test_mapper(self):
        mapper = self.mapper
        self.assertTrue(mapper)
        self.assertEqual(len(mapper), 3)
        self.assertEqual(mapper.user, mapper[User])
        managers = list(mapper)
        self.assertEqual(len(managers), 3)
        for manager in managers:
            self.assertIsInstance(manager, odm.Manager)
        self.assertEqual(mapper.default_store, self.store)
        self.assertFalse(mapper.valid_model({}))
        self.assertTrue(User in mapper)
        self.assertTrue(User._meta in mapper)
        self.assertTrue(str(mapper))

    def test_create_user(self):
        mapper = self.mapper
        user = yield from mapper.user(username='lsbardel').save()
        self.assertEqual(user.username, 'lsbardel')
        self.assertTrue(user.id)
        self.assertEqual(user.id, user.pk)
