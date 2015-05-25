from sqlalchemy import create_engine

import tests


@tests.green
class PostgreSqlTests(tests.TestCase):

    @classmethod
    def setupdb(cls, dbname):
        import odm
        engine = create_engine(cls.cfg.postgresql)
        conn = engine.connect()
        # the connection will still be inside a transaction,
        # so we have to end the open transaction with a commit
        conn.execute("commit")
        conn.execute('create database %s' % dbname)
        conn.close()
        return engine

    def test_create_task(self):
        with self.begin() as session:
            task = tests.Task(subject='simple task')
            session.add(task)
        self.assertTrue(task.id)

    def test_update_task(self):
        with self.begin() as session:
            task = tests.Task(subject='simple task to update')
            session.add(task)
        self.assertTrue(task.id)
        self.assertFalse(task.done)
        with self.begin() as session:
            task.done = True
            session.add(task)

        with self.begin() as session:
            task = session.query(tests.Task).get(task.id)
        self.assertTrue(task.done)
