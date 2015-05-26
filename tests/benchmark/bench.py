from asyncio import async, wait, TimeoutError
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
    name = "test_url"
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


# POOL_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024]
POOL_SIZES = [8, 16, 32, 64]
TIMEOUT = 15
REQUESTS = 1000


def wormup(worker, pool_size):
    worker.http = HttpClient(pool_size=pool_size, timeout=TIMEOUT)
    worker.requests = REQUESTS
    worker.logger.info('WORM UP')
    yield from request(worker)
    yield from worker.send('monitor', 'run', ready)


def bench(worker):
    worker.requests = 2*REQUESTS
    worker.logger.info('BENCHMARKING')
    yield from request(worker)


def request(worker):
    url = worker.cfg.test_url
    loop = worker._loop
    start = loop.time()
    number = worker.requests
    worker.logger.info('Sending %d requests with %d concurrency to "%s"',
                       number, worker.http.pool_size, url)
    requests = [worker.http.get(url) for _ in range(number)]
    done, pending = yield from wait(requests, loop=loop)
    assert not pending
    errors = 0
    for result in done:
        try:
            result.result()
        except Exception:
            errors += 1

    taken = loop.time() - start
    worker.logger.info('Processed %d requests with %d errors in %.3f',
                       number, errors, taken)
    return {'taken': taken,
            'number': number,
            'errors': errors}


def ready(monitor):
    monitor.ready += 1
    if monitor.ready == monitor.cfg.workers:
        for pool_size in POOL_SIZES:
            size = pool_size//monitor.cfg.workers
            if size*monitor.cfg.workers != pool_size:
                monitor.logger.error('Adjust workes so that pool sizes can be '
                                     'evenly shared across them')
                monitor._loop.stop()
            requests = [monitor.send(worker, 'run', wormup, size) for
                        worker in monitor.managed_actors]
            results = yield from wait(requests)
            requests = [monitor.send(worker, 'run', bench) for
                        worker in monitor.managed_actors]
            results = yield from wait(requests)
        monitor._loop.stop()


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
            worker._loop.call_later(1, async, wormup(worker, POOL_SIZES[0]))

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
