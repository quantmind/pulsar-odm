import unittest
import string
from functools import wraps

from pulsar.apps.test import random_string
from pulsar.apps.greenio import GreenPool

import odm
from odm.store import REV_KEY
from odm.green import GreenMapper
from odm.errors import QueryError

from .models import User, Session, Blog


def randomname():
    return random_string(min_len=8, max_len=8,
                         characters=string.ascii_letters)


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
        self.assertTrue(session.get(REV_KEY))
        # Add a value and save
        session['test'] = 'this is just a test'
        session2 = yield from session.save()
        self.assertEqual(session2, session)
        self.assertEqual(session2.test, 'this is just a test')
        self.assertEqual(session2.user_id, user.id)

    def test_get_session(self):
        mapper = self.mapper
        user = yield from mapper.user(username='lsbardel3').save()
        session = yield from mapper.session(user=user,
                                            test='hello').save()
        id = session.id
        session = yield from mapper.session.get(id)
        self.assertEqual(session.id, id)
        self.assertTrue(session.get(REV_KEY))
        self.assertEqual(session.user_id, user.id)
        self.assertFalse('_user' in session)
        user1 = yield from session.user
        self.assertEqual(user1, user)
        self.assertTrue('_user' in session)
        user2 = yield from session.user
        self.assertEqual(user2, user)

    def test_not_found(self):
        mapper = self.mapper
        yield from self.async.assertRaises(odm.ModelNotFound,
                                           mapper.user.get,
                                           'dkhsgcvshgcvshgcvsdh')

    def test_query(self):
        mapper = self.mapper
        user1 = yield from mapper.user(username='pluto1',
                                       email='pluto1@test.com').save()
        user2 = yield from mapper.user(username='pluto2',
                                       email='pluto2@test.com').save()
        query = mapper.user.query()
        self.assertIsInstance(query, odm.Query)
        self.assertEqual(query._manager, mapper.user)
        users = yield from query.all()
        self.assertTrue(users)
        self.assertTrue(len(users) >= 2)
        for user in users:
            self.assertTrue(user.get(REV_KEY))

    def test_filter_username(self):
        mapper = self.mapper
        user = yield from mapper.user(username='kappa',
                                      email='kappa@test.com').save()
        query = mapper.user.filter(username='kappa')
        self.assertIsInstance(query, odm.Query)
        users = yield from query.all()
        self.assertTrue(users)
        self.assertEqual(len(users), 1)
        user1 = users[0]
        self.assertTrue(user1.get(REV_KEY))
        self.assertEqual(user.id, user1.id)
        #
        # Test get filter
        kappa = yield from mapper.user.get(username='kappa')
        self.assertEqual(kappa.username, 'kappa')
        #
        yield from self.async.assertRaises(odm.ModelNotFound,
                                           mapper.user.get,
                                           username='kappaxxx')


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
        self.assertTrue(session.get(REV_KEY))

    @greenpool
    def test_get_session(self):
        mapper = self.mapper
        user = mapper.user(username='lsbardel3').save()
        session = mapper.session(user=user, test='hello').save()
        id = session.id
        session = mapper.session.get(id)
        self.assertEqual(session.id, id)
        self.assertTrue(session.get(REV_KEY))
        self.assertEqual(session.user_id, user.id)
        self.assertFalse('_user' in session)
        user1 = session.user
        self.assertEqual(user1, user)
        self.assertTrue('_user' in session)
        user2 = session.user
        self.assertEqual(user2, user)

    @greenpool
    def test_filter_username(self):
        mapper = self.mapper
        user = mapper.user(username='kappa',
                           email='kappa@test.com').save()
        query = mapper.user.filter(username='kappa')
        self.assertIsInstance(query, odm.Query)
        users = query.all()
        self.assertTrue(users)
        self.assertEqual(len(users), 1)
        user1 = users[0]
        self.assertTrue(user1.get(REV_KEY))
        self.assertEqual(user.id, user1.id)
        #
        # Test get filter
        kappa = mapper.user.get(username='kappa')
        self.assertEqual(kappa.username, 'kappa')
        #
        self.assertRaises(odm.ModelNotFound, mapper.user.get,
                          username='kappaxxx')
