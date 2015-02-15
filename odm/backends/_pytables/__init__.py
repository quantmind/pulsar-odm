
try:
    import tables as tb
except ImportError:
    tb = None


import odm


class PyTablesStore(odm.Store):

    @property
    def registered(self):
        return tables is not None

    def create_database(self, dbname=None, **kw):
        dbname = dbname or self._database
        if dbname:
            dbname = os.path.join(self)
            if self._h5f:
                self._h5f.close()
            self._h5f = tables.open_file('%s.h5' % dbname, 'w')
        else:
            raise ValueError('No database specified')

    def create_table(self, ):
        tbl = h5f.create_table('/', 'table_name', description_name)


odm.register_store("pytables", "odm.backends._pytables.PyTablesStore")
