from datetime import datetime

from sqlalchemy import Integer, Column, String, DateTime, Boolean

from pulsar.apps.greenio import GreenPool

import odm

__test__ = False


class Task(odm.Model):
    id = Column(Integer, primary_key=True)
    subject = Column(String(250))
    done = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)

    def __str__(self):
        return '%d: %s' % (self.id, self.subject)


def example(mp):
    # Make sure table is available
    mp.table_create()
    # Insert a new Task in the table
    with mp.begin() as session:
        task = mp.task(subject='my task')
        session.add(task)
    return task


if __name__ == '__main__':
    pool = GreenPool()
    mp = odm.Mapper('postgresql+green://odm:odmtest@127.0.0.1:5432/odmtests')
    mp.register(Task)
    task = pool._loop.run_until_complete(pool.submit(example, mp))
    print(task)
