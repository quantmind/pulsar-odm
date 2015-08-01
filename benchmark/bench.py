import csv
from asyncio import async, wait
from random import randint
from urllib.parse import urlparse
from functools import reduce

import pulsar
from pulsar.apps.http import HttpClient
from pulsar.apps.greenio import GreenPool
from pulsar.utils.slugify import slugify


POOL_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024]
POOL_SIZES = [8, 16, 32, 64, 128]
TIMEOUT = 120
REQUESTS = 10000
FIRST_WORMUP = 1000
FIELDNAMES = ['concurrency', 'requests', 'errors', 'time']


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


def wormup(worker, pool_size, total=FIRST_WORMUP):
    worker.http = HttpClient(pool_size=pool_size, timeout=TIMEOUT)
    worker.requests = total
    worker.logger.info('WORM UP')
    yield from request(worker, False)
    yield from worker.send('monitor', 'run', ready)


def bench(worker):
    worker.logger.info('BENCHMARKING')
    results = yield from request(worker)
    return results


def request(worker, log=True):
    url = worker.cfg.test_url
    loop = worker._loop
    number = worker.requests
    if log:
        worker.logger.info('Sending %d requests with %d concurrency to "%s"',
                           number, worker.http.pool_size, url)
    requests = [worker.http.get(url) for _ in range(number)]
    start = loop.time()
    done, pending = yield from wait(requests, loop=loop)
    taken = loop.time() - start
    assert not pending
    errors = 0
    for result in done:
        try:
            response = result.result()
            if response.status_code != 200:
                errors += 1
        except Exception:
            errors += 1

    if log:
        return {'time': taken,
                'requests': number,
                'errors': errors}


def add(name):
    return lambda a, b: a + b[name]


def ready(monitor):
    monitor.ready += 1
    if monitor.ready == monitor.cfg.workers:
        try:
            yield from run_benchmark(monitor)
        finally:
            monitor._loop.stop()


def run_benchmark(monitor):
    '''Run the benchmarks
    '''
    url = urlparse(monitor.cfg.test_url)
    name = slugify(url.path) or 'home'
    name = '%s_%d.csv' % (name, monitor.cfg.workers)
    monitor.logger.info('WRITING RESULTS ON "%s"', name)
    total = REQUESTS//monitor.cfg.workers

    with open(name, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        for pool_size in POOL_SIZES:
            size = pool_size//monitor.cfg.workers
            if size*monitor.cfg.workers != pool_size:
                monitor.logger.error('Adjust workes so that pool sizes '
                                     'can be evenly shared across them')
                monitor._loop.stop()

            # WORMUP
            requests = [monitor.send(worker, 'run', wormup, size, total) for
                        worker in monitor.managed_actors]
            yield from wait(requests)

            # BENCHMARK
            requests = [monitor.send(worker, 'run', bench) for
                        worker in monitor.managed_actors]
            results, pending = yield from wait(requests)
            assert not pending, 'Pending requets!'
            results = [r.result() for r in results]

            summary = {'concurrency': pool_size}
            for name in results[0]:
                summary[name] = reduce(add(name), results, 0)
            writer.writerow(summary)

            persec = summary['requests']/summary['time']
            monitor.logger.info('%d concurrency - %d requests - '
                                '%d errors - %.3f seconds - '
                                '%.2f requests/sec',
                                pool_size,
                                summary['requests'],
                                summary['errors'],
                                summary['time'],
                                persec)


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
