from pulsar import ProtocolConsumer, Connection


class Consumer(ProtocolConsumer):

    def start_request(self):
        pass

    def data_received(self, data):
        if self._response_buf:
             data = self._response_buf + data

        try:
            idx = data.index(b'\0')
            if data[:idx] != b"SUCCESS":
                raise RqlDriverError(
                    'Server dropped connection with message: "{}"'.format(
                        self.response_buf.decode('utf-8').strip()))

        except ValueError:
            self._response_buf = data


class _:

    def connection_made(self, transport):
        self.transport = transport
        self.response_buf = b''

        transport.write(b''.join((
            struct.pack(
                "<LL",
                p.VersionDummy.Version.V0_3,
                len(self.auth_key)),
            self.auth_key,
            struct.pack("<L", p.VersionDummy.Protocol.JSON))))

    def connection_lost(self):
        self.close(noreply_wait=False)

    def data_received(self, data):
        # We may get an async continue result, in which case we save it and
        # read the next response
        self.response_buf += data

        # Read out the response from the server, which will be a
        # null-terminated string
        if b"\0" in self.response_buf:
            idx = self.response_buf.index(b'\0')
            if self.response_buf[:idx] != b"SUCCESS":
                self.close(noreply_wait=False)
                raise RqlDriverError(
                    'Server dropped connection with message: "{}"'.format(
                        self.response_buf.decode('utf-8').strip()))
            self.response_buf = self.response_buf[idx + 1:]
            self.data_received = self.data_received_
            # Connection is now initialized
            self.data_received(b'')

    def data_received_(self, data):
        # We may get an async continue result, in which case we save it and
        # read the next response
        self.response_buf += data

        while True:
            if (not self.response_len) and (len(self.response_buf) >= 12):
                self.response_token, self.response_len = struct.unpack(
                    "<qL", self.response_buf[:12])
                self.response_buf = self.response_buf[12:]
            elif self.response_len and (
                    len(self.response_buf) >= self.response_len):
                response_token = self.response_token
                response_buf = self.response_buf[:self.response_len]

                # Construct response
                response = Response(response_token, response_buf)

                # Check that this is the response we were expecting
                if response_token not in self.cursor_cache:
                    # This response is corrupted or not intended for us.
                    raise RqlDriverError("Unexpected response received.")

                if self.cursor_cache[response_token]:
                    self.cursor_cache[response_token]._extend(response)

                self.response_buf = self.response_buf[self.response_len:]
                self.response_len = None

            else:
                break

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close(noreply_wait=False)

    def use(self, db):
        self.db = db

    def close(self, noreply_wait=True):
        if noreply_wait:
            self.noreply_wait()
        self.transport.close()
        for cursor in self.cursor_cache.values():
            cursor.close()

    def _cursor_closed(self, cursor):
        self.cursor_cache[cursor.query.token] = None

    def noreply_wait(self):
        for _ in self._send_query(Query(pQuery.NOREPLY_WAIT, None, None)):
            pass

    # Not thread safe. Sets this connection as global state that will be used
    # by subsequent calls to `query.run`. Useful for trying out RethinkDB in
    # a Python repl environment.
    def repl(self):
        RqlQuery._repl = self
        return self

    def _start(self, term, db=None, **global_optargs):
        # Set global opt args
        # The 'db' option will default to this connection's default
        # if not otherwise specified.
        global_optargs['db'] = DB(self.db) if db is None else DB(db)

        # Construct query
        query = Query(pQuery.START, term, global_optargs)

        return self._send_query(query, global_optargs)

    def _send_query(self, query, opts={}, async=False):
        # Error if this connection has closed
        if self.cursor_cache is None:
            raise RqlDriverError("Connection is closed.")

        query.accepts_r_json = True

        value = Cursor(self, query, opts)
        self.cursor_cache[query.token] = value

        # Send json
        query_str = query.serialize().encode('utf-8')
        query_header = struct.pack("<QL", query.token, len(query_str))
        self.transport.write(b''.join((query_header, query_str)))

        if opts.get('noreply') or async:
            value.close()
            return None

        return value

