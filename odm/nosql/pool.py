from functools import wraps

from sqlalchemy import pool

from greenlet import getcurrent

from pulsar import Pool, get_actor


def green(callable):
    '''Decorator for green functions and methods
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        current = getcurrent()
        parent = current.parent
        assert parent, 'running in main greenlet'
        return parent.switch(callable(*args, **kwargs))

    return _


def get_loop():
    return get_actor()._loop


class GreenPool(pool.Pool):

    def __init__(self, creator, pool_size=10, timeout=30, **kw):
        self._pool = Pool(creator, pool_size=pool_size, timeout=timeout)
        super().__init__(green(self._pool.connect), **kw)

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(self._pool._creator, pool_size=self.max_size(),
                              timeout=self.timeout(),
                              recycle=self._recycle, echo=self.echo,
                              logging_name=self._orig_logging_name,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              _dispatch=self.dispatch,
                              _dialect=self._dialect)

    def dispose(self):
        self._pool.close(False)

    def max_size(self):
        return self._pool.pool_size()

    def timeout(self):
        return self._pool._timeout

    def _do_get(self):
        return self._create_connection()

    def _do_return_conn(self, conn):
        self._pool._put(conn.connection)
