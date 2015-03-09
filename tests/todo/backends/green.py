
def green_task(method):
    '''Decorator to run a ``method`` an a new greenlet in the event loop
    of the instance of the bound ``method``.

    This method is the greenlet equivalent of the :func:`.task` decorator.
    The instance must be an :ref:`async object <async-object>`.

    :return: a :class:`~asyncio.Future`
    '''
    @wraps(method)
    def _(self, *args, **kwargs):
        future = Future(loop=self._loop)
        self._loop.call_soon_threadsafe(
            _green, self, method, future, args, kwargs)
        return future

    return _


def wait_fd(fd, read=True):
    '''Wait for an event on file descriptor ``fd``.

    :param fd: file descriptor
    :param read=True: wait for a read event if ``True``, otherwise a wait
        for write event.

    This function must be invoked from a coroutine with parent, therefore
    invoking it from the main greenlet will raise an exception.
    Check how this function is used in the :func:`.psycopg2_wait_callback`
    function.
    '''
    current = greenlet.getcurrent()
    parent = current.parent
    assert parent, '"wait_fd" must be called by greenlet with a parent'
    try:
        fileno = fd.fileno()
    except AttributeError:
        fileno = fd
    future = Future()
    # When the event on fd occurs switch back to the current greenlet
    if read:
        future._loop.add_reader(fileno, _done_wait_fd, fileno, future, read)
    else:
        future._loop.add_writer(fileno, _done_wait_fd, fileno, future, read)
    # switch back to parent greenlet
    parent.switch(future)
    return future.result()


# INTERNALS
def _done_wait_fd(fd, future, read):
    if read:
        future._loop.remove_reader(fd)
    else:
        future._loop.remove_writer(fd)
    future.set_result(None)


def _green(self, method, future, args, kwargs):
    # Called in the main greenlet
    try:
        gr = PulsarGreenlet(method)
        result = gr.switch(self, *args, **kwargs)
        if isinstance(result, Future):
            result.add_done_callback(partial(_green_check, gr, future))
        else:
            future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def _green_check(gr, future, fut):
    # Called in the main greenlet
    try:
        result = gr.switch(fut.result())
        if isinstance(result, Future):
            result.add_done_callback(partial(_green_check, gr, future))
        else:
            future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)
