import unittest

from odm import create_store

dns = 'rethinkdb://127.0.0.1:28015/testdb'


class RethinDbTest(unittest.TestCase):

    def test_store(self):
        store = create_store(dns)
        self.assertEqual(store.name, 'rethinkdb')
        self.assertEqual(store.database, 'testdb')
        self.assertEqual(store.dns, dns)

    def test_connect(self):
        store = create_store(dns)
        connection = yield from store.connect()
        self.assertEqual(connection.requests_processed, 1)
