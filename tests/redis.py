import tests

import odm


@tests.green
class PostgreSqlTests(tests.TestCase, tests.MapperMixin):

    @classmethod
    def url(cls):
        cls.dbname = '11'
        return 'redis+green://%s' % cls.cfg.redis_server
