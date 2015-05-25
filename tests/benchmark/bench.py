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


WORMUP = 256


class BenchWorker:

    def __init__(self, worker, cfg):
        self.worker = worker
        self.http = HttpClient()

    def start(self):
        self.worker.logger.info('Worm up')
        yield from self.request(WORMUP)

    def request(self, number):
        url = self.worker.cfg.test_url
        loop = self.worker._loop
        start = loop.time()
        self.worker.logger.info('Sending %d requests to "%s"', number, url)
        requests = [self.http.get(url) for _ in range(number)]
        resp = yield from wait(requests, loop=loop)
        taken = loop.time() - start
        self.worker.logger.info('Processed %d requests in %.3f',
                                number, taken)


class Bench(pulsar.Application):
    cfg = pulsar.Config(apps=['bench'])

    def monitor_start(self, monitor, exc=None):
        if monitor.cfg.filldb:
            self.pool = GreenPool()
            try:
                yield from self.pool.submit(self.filldb)
            finally:
                monitor._loop.stop()

    def worker_start(self, worker, exc=None):
        if not exc:
            bench = BenchWorker(worker, self.cfg)
            worker._loop.call_later(1, async, bench.start())

    def filldb(self):
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
            odm.logger('Created %d World models', todo)
        else:
            odm.logger('%d World models already available', N)


if __name__ == '__main__':
    Bench().start()
