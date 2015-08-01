

def single_result(name, result):
    return (name, result.__class__.__name__,
            None, None, None, None, None)


class NoSqlCursor:
    '''Abstract class for Cursors
    '''
    ProgrammingError = None
    '''DB API programming Error
    '''
    _statement_results = {}
    _executed = None
    _rowcount = None
    _rownumber = None
    _rows = None

    def __init__(self, connection):
        '''
        Do not create an instance of a Cursor yourself. Call
        connections.Connection.cursor().
        '''
        self.connection = connection

    @property
    def description(self):
        try:
            self._check_executed()
        except self.ProgrammingError:
            return None
        else:
            return self._executed

    @property
    def rowcount(self):
        try:
            self._check_executed()
        except self.ProgrammingError:
            return -1
        else:
            return len(self._result)

    @property
    def rownumber(self):
        return self._rownumber

    def close(self):
        raise NotImplementedError

    def execute(self, statement, parameters):
        raise NotImplementedError

    def executemany(self, statement, parameters):
        raise NotImplementedError

    def fetchone(self):
        self._check_executed()
        if self._rows is None or self._rownumber >= len(self._rows):
            return None
        result = self._rows[self._rownumber]
        self._rownumber += 1
        return result

    def _check_executed(self):
        if not self._executed:
            raise self.ProgrammingError("execute() first")

    def _set_execution(self, statement, result):
        if statement not in self._statement_results:
            description = single_result(statement, result)
            self._executed = (description,)
            self._rows = ((result,),)
        self._rownumber = 0
