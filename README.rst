:Badges: |license|  |pyversions| |status| |pypiversion|
:Master CI: |master-build|_ |coverage-master|
:Downloads: http://pypi.python.org/pypi/pulsar-odm
:Source: https://github.com/quantmind/pulsar-odm
:Mailing list: `google user group`_
:Design by: `Quantmind`_ and `Luca Sbardella`_
:Platforms: Linux, OSX, Windows. Python 3.5 and above
:Keywords: sql, sqlalchemy, asynchronous, asyncio, concurrency, greenlet


.. |pypiversion| image:: https://badge.fury.io/py/pulsar-odm.svg
  :target: https://pypi.python.org/pypi/pulsar-odm
.. |pyversions| image:: https://img.shields.io/pypi/pyversions/pulsar-odm.svg
  :target: https://pypi.python.org/pypi/pulsar-odm
.. |license| image:: https://img.shields.io/pypi/l/pulsar-odm.svg
  :target: https://pypi.python.org/pypi/pulsar-odm
.. |status| image:: https://img.shields.io/pypi/status/pulsar-odm.svg
  :target: https://pypi.python.org/pypi/pulsar-odm
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

.. contents:: **CONTENTS**


Usage
==========

To be able to use the object data mapper within standard blocking code,
one needs to use pulsar GreenPool_ as the following snippet highlights:

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

The ``example`` function is executed in a greenlet other than the main one. This is important otherwise the call fails:

.. code:: python

    >> example(mp)
    >> Traceback (most recent call last):
    ...
    RuntimeError: acquire in main greenlet

Running the function on the greenlet pool guarantees the correct asynchronous execution. When psycopg2_
executes a command against the database on a child greenlet, it switches control to the parent (main) greenlet, which is controlled by the asyncio eventloop so that other asynchronous operations can be carried out.
Once the result of the execution is ready, the execution switches back to the original child greenlet so that the ``example`` function can continue.

Testing
==========

To run tests, create a new role and database, for postgresql::

    psql -a -f tests/db.sql


Changelog
============

* `Version 0.6 <https://github.com/quantmind/pulsar-odm/blob/master/docs/history/0.6.md>`_
* `Version 0.5 <https://github.com/quantmind/pulsar-odm/blob/master/docs/history/0.5.md>`_
* `Version 0.4 <https://github.com/quantmind/pulsar-odm/blob/master/docs/history/0.4.md>`_
* `Versions pre 0.4 <https://github.com/quantmind/pulsar-odm/blob/master/docs/history/pre0.4.md>`_


.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _pulsar: http://pythonhosted.org/pulsar/
.. _sqlalchemy: http://www.sqlalchemy.org/
.. _greenlet: https://greenlet.readthedocs.org/en/latest/
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _psycopg2: http://pythonhosted.org/psycopg2/
.. _GreenPool: http://pythonhosted.org/pulsar/apps/greenio.html
