import sqlalchemy
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

        if 'pool_size' in u.query:
            kwargs['pool_size'] = int(u.query.pop('pool_size'))
        if 'pool_timeout' in u.query:
            kwargs['pool_timeout'] = float(u.query.pop('pool_timeout'))

        return super().create(name_or_url, **kwargs)


OdmEngineStrategy()
