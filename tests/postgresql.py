import tests


@tests.green
class PostgreSqlTests(tests.TestCase, tests.MapperMixin):

    @classmethod
    def url(cls):
        return cls.cfg.postgresql + '?pool_size=7&pool_timeout=15'

    def test_pool(self):
        from odm.backends.postgresql.pool import GreenletPool
        engine = self.mapper.get_engine()
        self.assertIsInstance(engine.pool, GreenletPool)
        self.assertEqual(engine.pool.max_size(), 7)
        self.assertEqual(engine.pool.timeout(), 15)
