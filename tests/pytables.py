import unittest

from odm import create_store


class TyTablesTest(unittest.TestCase):

    def test_store(self):
        store = create_store('pytables:///testdb')
        self.assertEqual(store.name, 'pytables')
        self.assertEqual(store.host, '')
        self.assertEqual(store.database, 'testdb')
