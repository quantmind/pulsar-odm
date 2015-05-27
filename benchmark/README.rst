Benchmark Pulsar and the Data Mapper
========================================

Server
============

The benchmark should be conducted on a single process server,
using multiple process simple add pwer linearly up to a limit given
by the number of cores in the machine.

To run the server as single process execute:
```
python3 app.py -w 0 --log-level warning
```


Benchmark
=============

To run benchmarks against the server:
```
python3 bench.py -w 2
```


Setup
==========

For testing postgreSQL create a new role::

    CREATE ROLE odm WITH PASSWORD 'odmtest';
    ALTER ROLE odm CREATEDB;
    ALTER ROLE odm LOGIN;
    CREATE DATABASE odmtests;
    GRANT ALL PRIVILEGES ON DATABASE odmtests to odm;
