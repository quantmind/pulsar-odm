import os
import sys
import json

from setuptools import setup, find_packages

os.environ['pulsar_odm_setup'] = 'yes'
odm = __import__('odm')


def read(fname):
    with open(fname) as f:
        return f.read()


def requirements():
    req = read('requirements.txt').replace('\r', '').split('\n')
    result = []
    for r in req:
        r = r.replace(' ', '')
        if r:
            result.append(r)
    return result


if __name__ == '__main__':
    setup(name='pulsar-odm',
          zip_safe=False,
          version=odm.__version__,
          author=odm.__author__,
          author_email=odm.__contact__,
          url=odm.__homepage__,
          license='BSD',
          description=odm.__doc__,
          long_description=read('README.rst'),
          packages=find_packages(exclude=('tests', 'tests.*')),
          install_requires=requirements(),
          classifiers=['Development Status :: 2 - Pre-Alpha',
                       'Environment :: Web Environment',
                       'Intended Audience :: Developers',
                       'License :: OSI Approved :: BSD License',
                       'Operating System :: OS Independent',
                       'Programming Language :: Python',
                       'Programming Language :: Python :: 3.4',
                       'Topic :: Utilities'])
