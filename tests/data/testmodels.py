import unittest
import string
from functools import wraps
from datetime import datetime, timedelta

import pytz

from pulsar.apps.test import random_string
from pulsar.apps.greenio import GreenPool

import odm
from odm.green import GreenMapper
from odm.errors import QueryError


def default_expiry(model):
    return datetime.now(pytz.utc) + timedelta(days=7)


def randomname():
    return random_string(min_len=8, max_len=8,
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
        return cls.store.database_drop()

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

    def test_query(self):
        mapper = self.mapper
        query = mapper.user.query()
        self.assertEqual(query._manager, mapper.user)
        self.assertEqual(query._meta, mapper.user._meta)
        self.assertEqual(query._mapper, mapper)
        self.assertEqual(query._loop, mapper.user._loop)
        query2 = query.filter(username='bla')
        self.assertNotEqual(query2, query)

    def test_query_errors(self):
        mapper = self.mapper
        yield from self.async.assertRaises(QueryError, mapper.user.get, 1, 2)

    def test_create_user(self):
        mapper = self.mapper
        user = yield from mapper.user(username='lsbardel').save()
        self.assertEqual(user.username, 'lsbardel')
        self.assertTrue(user.id)
        self.assertEqual(user.id, user.pk)
        # Let get the model
        user2 = yield from mapper.user.get(user.id)
        self.assertEqual(user2.id, user.id)

    def test_create_session(self):
        mapper = self.mapper
        user = yield from mapper.user(username='lsbardel').save()
        session = yield from mapper.session(user=user).save()
        self.assertTrue(session.id)
        self.assertTrue(session.expiry)
        self.assertEqual(session.user_id, user.id)


def greenpool(test):

    @wraps(test)
    def _(self):
        return self.pool.submit(test, self)

    return _


class GreenOdmTests(unittest.TestCase):
    models = (User, Session, Blog)

    @classmethod
    def create_mapper(cls, **kw):
        '''Create a mapper for models'''
        mapper = GreenMapper(cls.store)
        for model in cls.models:
            mapper.register(model)
        return mapper

    @classmethod
    def setUpClass(cls):
        cls.dbname = randomname()
        cls.store = cls.create_store()
        cls.pool = GreenPool()
        yield from cls.store.database_create(cls.dbname)
        cls.mapper = cls.create_mapper()
        yield from cls.pool.submit(cls.mapper.table_create)

    @classmethod
    def tearDownClass(cls):
        yield from cls.pool.shutdown()
        yield from cls.store.database_drop()

    @greenpool
    def test_create_user(self):
        mapper = self.mapper
        user = mapper.user(username='lsbardel').save()
        self.assertEqual(user.username, 'lsbardel')
        self.assertTrue(user.id)
        self.assertEqual(user.id, user.pk)

    @greenpool
    def test_create_session(self):
        mapper = self.mapper
        user = mapper.user(username='lsbardel').save()
        session = mapper.session(user=user).save()
        self.assertTrue(session.id)
        self.assertTrue(session.expiry)
        self.assertEqual(session.user_id, user.id)
