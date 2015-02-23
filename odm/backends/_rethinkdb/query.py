from rethinkdb import ast

from odm.query import CompiledQuery


class RethinkDbQuery(CompiledQuery):
    '''Implements the CompiledQuery for RethinkDB
    '''
    def _build(self):
        self.aggregated = None
        query = self._query
        if query._excludes:
            raise NotImplementedError
        if query._filters:
            self.aggregated = self.aggregate(query._filters)

    def all(self):
        table = ast.Table(self._meta.table_name)
        if self.aggregated:
            assert len(self.aggregated) == 1, ('Cannot filter on multiple '
                                               'lookups')
            name, lookups = list(self.aggregated.items())[0]
            values = None
            row = ast.ImplicitVar()
            for lookup in lookups:
                if lookup.type == 'value':
                    v = row[name] == lookup.value
                else:
                    raise NotImplementedError
                if values is None:
                    values = v
                else:
                    values = values & v

            field = self._meta.dfields.get(name)
            if field and field.index:
                raise NotImplementedError
            else:
                term = table.filter(values)
        else:
            term = table

        manager = self._manager
        store = self._store
        cursor = yield from store.execute(term)
        return [store._model_from_db(manager, **values) for values in cursor]
