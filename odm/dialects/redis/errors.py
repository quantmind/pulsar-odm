from pulsar.apps.data import redis

__all__ = ['DatabaseError', 'OperationalError', 'ProgrammingError']

DatabaseError = redis.RedisError


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass
