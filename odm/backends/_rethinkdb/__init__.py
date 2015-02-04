'''RethinkDB backend'''
try:
    import rethinkdb
except ImportError:
    rethinkdb = None

import odm


class RethinkDB(odm.Store):

    @property
    def registered(self):
        return rethinkdb is not None


odm.register_store("rethinkdb", "odm.backends._rethinkdb.RethinkDB")
