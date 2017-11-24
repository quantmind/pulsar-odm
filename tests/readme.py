from datetime import datetime

from sqlalchemy import Integer, Column, String, DateTime, Boolean

from pulsar.apps.greenio import GreenPool

from odm import mapper


class Item(mapper.Model):
    id = Column(Integer, primary_key=True)
    subject = Column(String(250))
    done = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)

    def __str__(self):
        return '%d: %s' % (self.id, self.subject)


def example(mp):
    # Make sure table is available
    mp.table_create()
    # Insert a new Item in the table
    with mp.begin() as session:
        item = mp.item(subject='my task')
        session.add(item)
    return item


def run():
    pool = GreenPool()
    mp = mapper.Mapper(
        'postgresql+green://odm:odmtest@127.0.0.1:5432/odmtests'
    )
    mp.register(Item)
    task = pool._loop.run_until_complete(pool.submit(example, mp))
    print(task)


if __name__ == '__main__':
    run()
