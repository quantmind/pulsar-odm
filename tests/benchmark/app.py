from contextlib import contextmanager
from random import randint

from pulsar.apps import wsgi
from pulsar.apps.wsgi import route, Json, AsyncString

from sqlalchemy import Column, Integer, String

import odm


MAXINT = 10000


class World(odm.Model):
    id = Column(Integer, primary_key=True)
    randomNumber = Column(Integer)


class Fortune(odm.Model):
    id = Column(Integer, primary_key=True)
    message = Column(String)


class Router(wsgi.Router):

    @route()
    def json(self, request):
        return Json({'message': "Hello, World!"}).http_response(request)

    @route()
    def plaintext(self, request):
        return AsyncString('Hello, World!').http_response(request)

    @route()
    def db(self, request):
        '''Single Database Query'''
        with self.begin() as session:
            world = session.query(World).get(randint(1, 10000))
        return Json(self.get_json(world)).http_response(request)

    @route()
    def queries(self, request):
        '''Multiple Database Queries'''
        queries = self.get_queries(request)
        worlds = []
        with self.begin() as session:
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
            with self.begin() as session:
                world = session.query(World).get(randint(1, MAXINT))
                world.randomNumber = randint(1, MAXINT)
                session.add(world)
            worlds.append(self.get_json(world))
        return Json(worlds).http_response(request)

    @contextmanager
    def begin(self, close=True, expire_on_commit=False, **options):
        """Provide a transactional scope around a series of operations.

        By default, ``expire_on_commit`` is set to False so that instances
        can be used outside the session.
        """
        session = self.session(expire_on_commit=expire_on_commit, **options)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            if close:
                session.close()

    def get_queries(self, request):
        queries = request.urlargs.get("queries", "1")
        try:
            queries = int(queries.strip())
        except ValueError:
            queries = 1

        return min(max(1, queries), 500)

    def get_json(self, world):
        return {'id': world.id, 'randomNumber': world.randomNumber}


class Site(wsgi.LazyWsgi):

    def setup(self, environ):
        return wsgi.WsgiHandler([wsgi.wait_for_body_middleware,
                                 Router('/')],
                                async=True)


def server(description=None, **kwargs):
    description = description or 'Pulsar Benchmark'
    return wsgi.WSGIServer(Site(), description=description, **kwargs)


if __name__ == '__main__':
    server().start()
