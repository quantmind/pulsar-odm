import tests

__test__ = False


@tests.green
class RedisTests(tests.TestCase):
    # class RedisTests(tests.TestCase, tests.MapperMixin):

    @classmethod
    def url(cls):
        cls.dbname = '11'
        return 'redis+green://%s' % cls.cfg.redis_server

    def test_load(self):
        engine = self.mapper.get_engine()
        result = engine.execute('SCRIPT LOAD', 'local a=2;')
        self.assertTrue(result)
