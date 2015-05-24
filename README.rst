Pulsar Object data mapper
===============================

Requires python 3.4 or above.

.. image:: https://travis-ci.org/quantmind/pulsar-odm.svg?branch=master
    :target: https://travis-ci.org/quantmind/pulsar-odm

.. image:: https://coveralls.io/repos/quantmind/pulsar-odm/badge.svg
  :target: https://coveralls.io/r/quantmind/pulsar-odm


Testing
==========

For testing postgreSQL create a new role::

    CREATE ROLE odm WITH PASSWORD 'odmtest';
    ALTER ROLE odm CREATEDB;
    ALTER ROLE odm LOGIN;
    CREATE DATABASE odmtests;
    GRANT ALL PRIVILEGES ON DATABASE odmtests to odm;
