import tests

import odm


@tests.green
class PostgreSqlTests(tests.TestCase, tests.MapperMixin):

    @classmethod
    def url(cls):
        return cls.cfg.postgresql

    def test_create_task(self):
        with self.mapper.begin() as session:
            task = tests.Task(subject='simple task')
            session.add(task)
        self.assertTrue(task.id)

    def test_update_task(self):
        with self.mapper.begin() as session:
            task = tests.Task(subject='simple task to update')
            session.add(task)
        self.assertTrue(task.id)
        self.assertFalse(task.done)
        with self.mapper.begin() as session:
            task.done = True
            session.add(task)

        with self.mapper.begin() as session:
            task = session.query(tests.Task).get(task.id)
        self.assertTrue(task.done)
