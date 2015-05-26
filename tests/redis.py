import tests

import odm

__test__ = False


@tests.green
class PostgreSqlTests(tests.TestCase):

    @classmethod
    def url(cls):
        cls.dbname = '11'
        return 'redis://%s' % cls.cfg.redis_server

    def test_create_task(self):
        with self.begin() as session:
            task = tests.Task(subject='simple task')
            session.add(task)
        self.assertTrue(task.id)
