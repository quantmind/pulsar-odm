from tests.data import tests

from odm import create_store


class RethinDbOdmTest(tests.OdmTests):

    @classmethod
    def create_store(cls):
        return create_store('rethinkdb://127.0.0.1:28015')
