from sqlalchemy.engine.default import DefaultDialect


class NoSqlDialect(DefaultDialect):
    name = 'nosql'
