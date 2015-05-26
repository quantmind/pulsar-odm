from asyncio import Future
from collections import deque

from sqlalchemy import pool

import pulsar
from pulsar.apps.greenio import wait


class Pool(pulsar.Pool):

    def connect(self):
        assert not self._closed
        return wait(self._get())

    def is_connection_closed(self, connection):
        if hasattr(connection, 'sock'):     # pragma    nocover
            if is_socket_closed(connection.sock):
                connection.close()
                return True
        return False


class AsyncPool(pool.Pool):
    '''Asynchronous Pool of connections.

    This pool should always be accessed in thread
    '''
    def __init__(self, creator, pool_size=10, timeout=30, loop=None, **kw):
        self._pool = Pool(creator, pool_size=pool_size, timeout=timeout,
                          loop=loop)
        super().__init__(self._pool.connect, **kw)

    @property
    def _loop(self):
        return self._pool._loop

    def dispose(self):
        self._pool.close()
        self.logger.info("Pool disposed. %s", self.status())

    def status(self):
        return self._pool.status()

    def size(self):
        return self._pool.pool_size

    def timeout(self):
        return self._pool._timeout

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(self._pool._creator, pool_size=self.size(),
                              timeout=self._pool._timeout, loop=self._loop,
                              recycle=self._recycle, echo=self.echo,
                              logging_name=self._orig_logging_name,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              _dispatch=self.dispatch,
                              _dialect=self._dialect)

    def _do_return_conn(self, rec):
        return wait(self._pool._put(rec.connection))

    def _do_get(self):
        return self._create_connection()
