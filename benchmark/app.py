from random import randint

import pulsar
from pulsar.apps import wsgi
from pulsar.apps.wsgi import route, Json, AsyncString
from pulsar.apps.greenio import wait, WsgiGreen

from sqlalchemy import Column, Integer, String

import odm


GREEN_POOL = 10
MAXINT = 10000


class Engine(pulsar.Setting):
    app = 'socket'
    name = 'engine'
    flags = ['--engine']
    meta = "CONNECTION_STRING"
    default = ('postgresql+async://odm:odmtest@127.0.0.1:5432/odmtests'
               '?pool_timeout=15')
    desc = 'Default connection string for the Backend database server'


class World(odm.Model):
    id = Column(Integer, primary_key=True)
    randomNumber = Column(Integer)


class Fortune(odm.Model):
    id = Column(Integer, primary_key=True)
    message = Column(String)


class Router(wsgi.Router):

    def get(self, request):
        '''Simply list test urls
        '''
        data = {}
        for route in self.routes:
            data[route.name] = request.absolute_uri(route.path())
        return Json(data).http_response(request)

    @route()
    def json(self, request):
        return Json({'message': "Hello, World!"}).http_response(request)

    @route()
    def plaintext(self, request):
        return AsyncString('Hello, World!').http_response(request)

    @route()
    def db(self, request):
        '''Single Database Query'''
        with self.mapper.begin() as session:
            world = session.query(World).get(randint(1, 10000))
        return Json(self.get_json(world)).http_response(request)

    @route()
    def queries(self, request):
        '''Multiple Database Queries'''
        queries = self.get_queries(request)
        worlds = []
        with self.mapper.begin() as session:
            for _ in range(queries):
                world = session.query(World).get(randint(1, MAXINT))
                worlds.append(self.get_json(world))
        return Json(worlds).http_response(request)

    @route()
    def updates(self, request):
        '''Multiple updates'''
        queries = self.get_queries(request)
        worlds = []
        for _ in range(queries):
            with self.mapper.begin() as session:
                world = session.query(World).get(randint(1, MAXINT))
                world.randomNumber = randint(1, MAXINT)
                session.add(world)
            worlds.append(self.get_json(world))
        return Json(worlds).http_response(request)

    def get_queries(self, request):
        queries = request.url_data.get("queries", "1")
        try:
            queries = int(queries.strip())
        except ValueError:
            queries = 1

        return min(max(1, queries), 500)

    def get_json(self, world):
        return {'id': world.id, 'randomNumber': world.randomNumber}


class Site(wsgi.LazyWsgi):

    def setup(self, environ):
        cfg = environ['pulsar.cfg']
        mapper = odm.Mapper(cfg.engine)
        #
        # Register the two models
        mapper.register(World)
        mapper.register(Fortune)
        #
        route = Router('/', mapper=mapper)
        #
        # Concurrency method
        if mapper.green_pool:
            # Use pool of greenlets
            pool = WsgiGreen(route, mapper.green_pool)
        else:
            # Use pool of threads
            pool = wsgi.middleware_in_executor(route)
        return wsgi.WsgiHandler((wsgi.wait_for_body_middleware, pool),
                                async=True)


def server(description=None, **kwargs):
    description = description or 'Pulsar Benchmark'
    return wsgi.WSGIServer(Site(), description=description, **kwargs)


if __name__ == '__main__':
    server().start()
