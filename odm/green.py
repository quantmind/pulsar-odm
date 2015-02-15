from inspect import ismethod

from pulsar import is_async
from pulsar.apps.greenio import wait

from .mapper import Mapper


class GreenMapper(Mapper):
    '''A :class:`~odm.Mapper` which runs asynchronous method on a
    greenlet pool
    '''
    def _register(self, manager):
        super()._register(GreenManager(manager))

    def table_create(self, remove_existing=False):
        return wait(super().table_create(remove_existing))

    def table_drop(self):
        return wait(super().table_drop())


class GreenManager:

    def __init__(self, manager):
        self.manager = manager

    def __getattr__(self, name):
        attr = getattr(self.manager, name)
        return greentask(attr) if ismethod(attr) else attr

    def __call__(self, *args, **kwargs):
        '''Create a new model without committing to database.
        '''
        return self._store.create_model(self, *args, **kwargs)


class greentask:
    __slots__ = ('_callable',)

    def __init__(self, callable):
        self._callable = callable

    def __call__(self, *args, **kw):
        coro = self._callable(*args, **kw)
        return wait(coro) if is_async(coro) else coro

    def __repr__(self):
        return self._callable.__repr__()
