import unittest

from odm import create_store

from tests.data.tests import randomname


class PyTableTests(unittest.TestCase):

    @classmethod
    def create_store(cls):
        return create_store('pytables:///%s' % randomname())

    @classmethod
    def setUpClass(cls):
        cls.store = cls.create_store()
        yield from cls.store.database_create()

    @classmethod
    def tearDownClass(cls):
        yield from cls.store.database_drop()

    def test_store(self):
        store = self.store
        self.assertEqual(store.name, 'pytables')
