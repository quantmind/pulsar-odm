from asyncio import async, wait
from random import randint

import pulsar
from pulsar.apps.http import HttpClient
from pulsar.apps.greenio import GreenPool


class PostgreSql(pulsar.Setting):
    app = 'bench'
    meta = "CONNECTION_STRING"
    default = 'postgresql+async://odm:odmtest@127.0.0.1:5432/odmtests'
    desc = 'Default connection string for the PostgreSql server'


class TestUrl(pulsar.Setting):
    app = 'bench'
    name= "test_url"
    default = "http://127.0.0.1:8060/json"
    flags = ["--test-url"]
    desc = "url to test"


class FillDB(pulsar.Setting):
    app = 'bench'
    name = 'filldb'
    flags = ['--filldb']
    default = False
    action = 'store_true'
    desc = "Fill database with random data"


POOL_SIZE = 256


def wormup(worker):
    worker.http = HttpClient(pool_size=POOL_SIZE)
    worker.requests = 2*POOL_SIZE
    worker.logger.info('Worm up')
    yield from request(worker)
    yield from worker.send('monitor', 'run', ready)


def bench(worker):
    worker.requests += 2*POOL_SIZE
    worker.logger.info('Benchmarking')
    yield from request(worker)


def request(worker):
    url = worker.cfg.test_url
    loop = worker._loop
    start = loop.time()
    number = worker.requests
    worker.logger.info('Sending %d requests to "%s"', number, url)
    requests = [worker.http.get(url) for _ in range(number)]
    resp = yield from wait(requests, loop=loop)
    taken = loop.time() - start
    worker.logger.info('Processed %d requests in %.3f', number, taken)
    return {'taken': taken,
            'number': number}


def ready(monitor):
    monitor.ready += 1
    if monitor.ready == monitor.cfg.workers:
        monitor.logger.info('Start benchmarks')
        requests = [monitor.send(worker, 'run', bench) for
                    worker in monitor.managed_actors]
        results = yield from wait(requests)



class Bench(pulsar.Application):
    cfg = pulsar.Config(apps=['bench'])

    def monitor_start(self, monitor, exc=None):
        if monitor.cfg.filldb:
            self.pool = GreenPool()
            try:
                yield from self.pool.submit(self.filldb)
            finally:
                monitor._loop.stop()
        else:
            monitor.ready = 0

    def worker_start(self, worker, exc=None):
        if not exc:
            worker._loop.call_later(1, async, wormup(worker))

    def filldb(self):
        '''Fill database
        '''
        from app import World, Fortune, odm, MAXINT

        mapper = odm.Mapper(self.cfg.postgresql)
        mapper.register(World)
        mapper.register(Fortune)
        mapper.table_create()

        with mapper.begin() as session:
            query = session.query(mapper.world)
            N = query.count()
            todo = max(0, MAXINT - N)
            if todo:
                for _ in range(todo):
                    world = mapper.world(randomNumber=randint(1, MAXINT))
                    session.add(world)

        if todo:
            odm.logger.info('Created %d World models', todo)
        else:
            odm.logger.info('%d World models already available', N)


if __name__ == '__main__':
    Bench().start()
