import unittest
import string

from pulsar.apps.test import random_string

from odm import create_store


randomname = lambda : random_string(min_len=8, max_len=8,
                                    characters=string.ascii_letters)


class RethinDbTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbname = randomname()
        cls.store = create_store('rethinkdb://127.0.0.1:28015')
        yield from cls.store.database_create(cls.dbname)

    @classmethod
    def tearDownClass(cls):
        yield from cls.store.database_drop()

    def test_store(self):
        store = self.store
        self.assertEqual(store.name, 'rethinkdb')
        self.assertEqual(store.database, self.dbname)
        self.assertEqual(store.dns,
                         'rethinkdb://127.0.0.1:28015/%s' % self.dbname)

    def test_all_create_drop_database(self):
        store = create_store('rethinkdb://127.0.0.1:28015')
        name = randomname()
        dbname = yield from store.database_create(name)
        self.assertEqual(store.database, name)
        dblist = yield from store.database_all()
        self.assertTrue(name in dblist)
        res = yield from store.database_drop(name)
        dblist = yield from store.database_all()
        self.assertFalse(name in dblist)

    def test_connect(self):
        store = self.store
        connection = yield from store.connect()
        self.assertEqual(connection.requests_processed, 1)

    def test_db_list(self):
        store = self.store
        dblist = yield from store.database_all()
        self.assertIsInstance(dblist, list)
        self.assertTrue('rethinkdb' in dblist)

    def test_all_create_delete_table(self):
        store = self.store
        tables = yield from store.table_all()
        self.assertIsInstance(tables, list)
        name = randomname()
        self.assertFalse(name in tables)
        # Create a table
        table = yield from store.table_create(name)
        self.assertTrue(table)
        #
        tables = yield from store.table_all()
        self.assertTrue(name in tables)
