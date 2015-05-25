import asyncio
from functools import partial

import pulsar
from pulsar.apps.ds import redis_parser
from pulsar.apps.data import redis, create_store

from odm.nosql import green, get_loop, wait, Engine


class Connection(pulsar.Connection):

    def __init__(self, **kw):
        super().__init__(redis.Consumer, **kw)
        self.parser = self._producer._parser_class()

    @property
    def description(self):
        return None

    def begin(self):
        return self

    def execute(self, *args, **options):
        consumer = self.current_consumer()
        consumer.start((args, options))
        result = yield from consumer.on_finished
        if isinstance(result, redis.ResponseError):
            raise result.exception
        return result

    def cursor(self):
        return self


class RedisEngine(Engine):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = create_store(str(self.url),
                                  protocol_factory=Connection)

    def execute(self, object, *multiparams, **params):
        pass

    @green
    def connect(self):
        connection = self.store.connect()
