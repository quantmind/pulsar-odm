from sqlalchemy import pool

from pulsar.apps.greenio import GreenLock


class GreenletPool(pool.Pool):
    '''A Pool that maintains one connection per greenlet.

    Maintains one connection per each greenlet, never moving a
    connection to a greenlet other than the one which it was
    created in.
    '''
    def __init__(self, creator, pool_size=10, timeout=30, **kw):
        super().__init__(creator, **kw)
        self._lock = GreenLock()
        self._max_size = pool_size
        self._timeout = timeout
        self._connections = set()
        self._available_connections = set()

    def dispose(self):
        for conn in self._connections:
            try:
                conn.close()
            except Exception:
                # pysqlite won't even let you close a conn from a thread
                # that didn't create it
                pass
        self.logger.info("Pool disposed. %s", self.status())

    def status(self):
        return "size: %d, available: %d" % (self.size(),
                                            len(self._available_connections))

    def size(self):
        return len(self._connections)

    def max_size(self):
        return self._max_size

    def timeout(self):
        return self._timeout

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(self._creator,
                              pool_size=self.max_size,
                              recycle=self._recycle,
                              echo=self.echo,
                              logging_name=self._orig_logging_name,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              _dispatch=self.dispatch,
                              dialect=self._dialect)

    def _do_return_conn(self, conn):
        self._available_connections.add(conn)

    def _do_get(self):
        try:
            return self._available_connections.pop()
        except KeyError:
            pass

        # Only create one connection at a time, otherwise psycopg2 block!
        with self._lock:
            conn = self._create_connection()
            self._connections.add(conn)
            return conn
