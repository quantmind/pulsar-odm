Pulsar Object data mapper
===============================

:Master CI: |master-build|_ |coverage-master|
:Dev CI: |dev-build|_ |coverage-dev|
:Downloads: http://pypi.python.org/pypi/pulsar-odm
:Source: https://github.com/quantmind/pulsar-odm
:Mailing list: `google user group`_
:Design by: `Quantmind`_ and `Luca Sbardella`_
:Platforms: Linux, OSX, Windows. Python 3.4 and above
:Keywords: sql, redis, sqlalchemy, asynchronous, concurrency, thread


.. |master-build| image:: https://travis-ci.org/quantmind/pulsar-odm.svg?branch=master
.. _master-build: http://travis-ci.org/quantmind/pulsar-odm
.. |dev-build| image:: https://travis-ci.org/quantmind/pulsar-odm.svg?branch=dev
.. _dev-build: http://travis-ci.org/quantmind/pulsar-odm
.. |coverage-master| image:: https://coveralls.io/repos/quantmind/pulsar-odm/badge.svg
  :target: https://coveralls.io/r/quantmind/pulsar-odm?branch=master
.. |coverage-dev| image:: https://img.shields.io/coveralls/quantmind/pulsar-odm/dev.svg
  :target: https://coveralls.io/r/quantmind/pulsar-odm?branch=dev

Pulsar-odm is build on top of pulsar_, sqlalchemy_ and greenlet_ libraries to
provide an implicit asynchronous object data mapper to use with code written
with asyncio_.
Currently only one dialect is implemented and tested:

* postgres+green, postgresql dialect with psycopg2_ and greenlet_

Usage
==========

The engine is created using sqlalchemy api::

    eg = engine('postgresql+green://...')

To be able to use the object data mapper within standard blocking code,
one need to use pulsar GreenPool_:

.. code:: python

    from datetime import datetime

    from sqlalchemy import Integer, Column, String, DateTime, Boolean
    
    from pulsar.apps.greenio import GreenPool
    
    import odm
    
    
    class Task(odm.Model):
        id = Column(Integer, primary_key=True)
        subject = Column(String(250))
        done = Column(Boolean, default=False)
        created = Column(DateTime, default=datetime.utcnow)
    
    def example(mapper):
        with mapper.begin() as session:
            task = mapper.task(subject='my task')
        return task

    in __name__ == '__main__':
        pool = GreenPool()
        mapper = odm.mapper('postgresql+green://...')
        mapper.register(Task)
        task = pool._loop.run_until_complete(pool.submit(example, mapper))


Testing
==========

For testing postgreSQL create a new role::

    CREATE ROLE odm WITH PASSWORD 'odmtest';
    ALTER ROLE odm CREATEDB;
    ALTER ROLE odm LOGIN;
    CREATE DATABASE odmtests;
    GRANT ALL PRIVILEGES ON DATABASE odmtests to odm;


.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _pulsar: http://pythonhosted.org/pulsar/
.. _sqlalchemy: http://www.sqlalchemy.org/
.. _greenlet: https://greenlet.readthedocs.org/en/latest/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _psycopg2: http://pythonhosted.org/psycopg2/
.. _GreenPool: http://pythonhosted.org/pulsar/apps/greenio.html
