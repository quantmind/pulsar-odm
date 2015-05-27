from asyncio import Future

from greenlet import getcurrent
from psycopg2 import *

from pulsar import ImproperlyConfigured


def green_connect(green_pool, *args, **kwargs):
    current = getcurrent()
    if not current.parent:
        # we are already in the main greenlet.
        assert green_pool, "Green pool not available, cannot connect"
        return green_pool.submit(connect, *args, **kwargs)
    else:
        # On a child greenlet simply connect
        return connect(*args, **kwargs)


def psycopg2_wait_callback(conn):
    """A wait callback to allow greenlet to work with Psycopg.
    The caller must be from a greenlet other than the main one.

    :param conn: psycopg2 connection or file number

    This function must be invoked from a coroutine with parent, therefore
    invoking it from the main greenlet will raise an exception.
    """
    while True:
        state = conn.poll()
        if state == extensions.POLL_OK:
            # Done with waiting
            break
        elif state == extensions.POLL_READ:
            _wait_fd(conn)
        elif state == extensions.POLL_WRITE:
            _wait_fd(conn, read=False)
        else:  # pragma    nocover
            raise OperationalError("Bad result from poll: %r" % state)


# INTERNALS

def _wait_fd(conn, read=True):
    '''Wait for an event on file descriptor ``fd``.

    :param conn: file descriptor
    :param read: wait for a read event if ``True``, otherwise a wait
        for write event.

    This function must be invoked from a coroutine with parent, therefore
    invoking it from the main greenlet will raise an exception.
    '''
    current = getcurrent()
    parent = current.parent
    assert parent, '"_wait_fd" must be called by greenlet with a parent'
    try:
        fileno = conn.fileno()
    except AttributeError:
        fileno = conn
    future = Future()
    # When the event on fd occurs switch back to the current greenlet
    if read:
        future._loop.add_reader(fileno, _done_wait_fd, fileno, future, read)
    else:
        future._loop.add_writer(fileno, _done_wait_fd, fileno, future, read)
    # switch back to parent greenlet
    parent.switch(future)
    # Back on the child greenlet. Raise error if there is one
    future.result()


def _done_wait_fd(fd, future, read):
    if read:
        future._loop.remove_reader(fd)
    else:
        future._loop.remove_writer(fd)
    future.set_result(None)


try:
    extensions.POLL_OK
except AttributeError:  # pragma    nocover
    raise ImproperlyConfigured(
        'Psycopg2 does not have support for asynchronous connections. '
        'You need at least version 2.2.0 of Psycopg2.')

extensions.set_wait_callback(psycopg2_wait_callback)