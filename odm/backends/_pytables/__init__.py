import os

try:
    import tables as tb
except ImportError:     # pragma    nocover
    tb = None


import odm


class PyTablesStore(odm.Store):

    @classmethod
    def register(cls):
        assert tb is not None, 'Requires pytables'

    def database_create(self, dbname=None, **kw):
        dbname = dbname or self._database
        if dbname:
            dbname = os.path.join(self.dbpath, dbname)
            if self._h5f:
                self._h5f.close()
            dbname = '%s.h5' % dbname
            self._h5f = tb.open_file(dbname, 'w')
        else:
            raise ValueError('No database specified')
        return dummy_coro(dbname)

    def database_drop(self, dbname=None, **kw):
        dbname = dbname or self._database
        if dbname:
            dbname = os.path.join(self.dbpath, dbname)
            dbname = '%s.h5' % dbname
            if self._h5f and self._h5f.filename == dbname:
                self._h5f.close()
                self._h5f = None
            os.remove(dbname)
        else:
            raise ValueError('No database specified')
        return dummy_coro(dbname)

    def create_table(self, ):
        tbl = h5f.create_table('/', 'table_name', description_name)

    def _init(self, dbpath='', **kwargs):
        self._h5f = None
        self.dbpath = dbpath


def dummy_coro(result=None):
    if False:
        yield None
    return result


odm.register_store("pytables", "odm.backends._pytables.PyTablesStore")
