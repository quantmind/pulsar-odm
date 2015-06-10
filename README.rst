Pulsar Object data mapper
===============================

:Master CI: |master-build|_ |coverage-master|
:Dev CI: |dev-build|_ |coverage-dev|
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
