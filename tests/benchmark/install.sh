#!/bin/bash
export PY3_ROOT=$IROOT/py3
export PY3=$PY3_ROOT/bin/python3
export PY3_PIP=$PY3_ROOT/bin/pip3

mkdir -p $IROOT/.pip_cache
export PIP_DOWNLOAD_CACHE=$IROOT/.pip_cache

fw_depends python2 python3

$PY3_PIP install --install-option="--prefix=${PY3_ROOT}" -r $TROOT/requirements.txt
