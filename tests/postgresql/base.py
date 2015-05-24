import tests


@tests.green
class PostgreSqlTests(tests.TestCase):

    @classmethod
    def setupdb(cls, dbname):
        import odm
        engine = odm.create_engine(cls.cfg.postgresql)
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
