#!/bin/bash
export PY3_ROOT=$IROOT/py3
export PY3=$PY3_ROOT/bin/python3

$PY3 app.py --bind :8080 --postgres=${DBHOST} --log-level error &
