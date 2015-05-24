import sqlalchemy
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.engine.strategies import DefaultEngineStrategy

from pulsar.apps.greenio import wait


class GreenConnection(Connection):
    pass


class GreenEngine(Engine):
    _connection_cls = GreenConnection


class GreenEngineStrategy(DefaultEngineStrategy):
    """Strategy for configuring an Engine with threadlocal behavior."""

    name = 'green'
    engine_cls = GreenEngine


GreenEngineStrategy()


def create_engine(*args, strategy=None, **kwargs):
    kwargs.setdefault('strategy', 'green')
    return sqlalchemy.create_engine(*args, **kwargs)
