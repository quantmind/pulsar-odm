import unittest

from odm import create_store


class RethinDbTest(unittest.TestCase):

    def test_store(self):
        store = create_store('rethinkdb://192.168.0.5:5000/testdb')
        self.assertEqual(store.name, 'rethinkdb')
        self.assertEqual(store.database, 'testdb')
