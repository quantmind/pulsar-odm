from pulsar.apps import data

from odm.store import OdmMixin, Command


class RedisStore(data.RedisStore, OdmMixin):

    def execute_transaction(self, transaction):
        '''Execute a :class:`.Transaction`
        '''
        models = []
        pipe = self.pipeline()
        update_insert = set((Command.INSERT, Command.UPDATE))
        #
        for command in transaction.commands:
            action = command.action
            if not action:
                pipe.execute(*command.args)
            elif action in update_insert:
                model = command.args
                model['_rev'] = model.get('_rev', 0) + 1
                models.append(model)
                key = self.basekey(model._meta, model.id)
                pipe.hmset(key, self.model_data(model, action))
            else:
                raise NotImplementedError
        yield from pipe.commit()
        return models

    def get_model(self, manager, pk):
        key = '%s%s:%s' % (self.namespace, manager._meta.table_name,
                           to_string(pk))
        return self.execute('hgetall', key,
                            factory=partial(self.build_model, manager))

    def compile_query(self, query):
        compiled = CompiledQuery(self.pipeline())
        return compiled


register_store('redis', 'odm.backends._redis.RedisStore')
