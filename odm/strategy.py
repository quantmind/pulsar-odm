import sqlalchemy
from sqlalchemy import util
from sqlalchemy.engine import url
from sqlalchemy.engine.strategies import PlainEngineStrategy


def create_engine(*args, **kwargs):
    kwargs.setdefault('strategy', 'odm')
    return sqlalchemy.create_engine(*args, **kwargs)


class OdmEngineStrategy(PlainEngineStrategy):
    name = 'odm'

    def create(self, name_or_url, **kwargs):
        # create url.URL object
        u = url.make_url(name_or_url)
        dialect_cls = u.get_dialect()

        if 'pool_size' in u.query:
            kwargs['pool_size'] = int(u.query.pop('pool_size'))
        if 'timeout' in u.query:
            kwargs['timeout'] = float(u.query.pop('timeout'))

        return super().create(name_or_url, **kwargs)


OdmEngineStrategy()
