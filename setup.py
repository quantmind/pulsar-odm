#!/usr/bin/env python
import sys
import os
import json
import subprocess

from setuptools import setup, find_packages


def extend(params, package=None):
    if package:
        path = os.path.dirname(__file__)
        data = sh('%s %s package_info %s %s'
                  % (sys.executable, __file__, package, path))
        params.update(json.loads(data))

    return params


def read(name):
    filename = os.path.join(os.path.dirname(__file__), name)
    with open(filename) as fp:
        return fp.read()


def requirements(name):
    install_requires = []
    dependency_links = []

    for line in read(name).split('\n'):
        if line.startswith('-e '):
            link = line[3:].strip()
            if link == '.':
                continue
            dependency_links.append(link)
            line = link.split('=')[1]
        line = line.strip()
        if line:
            install_requires.append(line)

    return install_requires, dependency_links


def sh(command):
    return subprocess.Popen(command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            universal_newlines=True).communicate()[0]


def package_info():
    package = sys.argv[2]
    if len(sys.argv) > 3:
        sys.path.append(sys.argv[3])
    os.environ['package_info'] = package
    pkg = __import__(package)
    print(json.dumps(dict(version=pkg.__version__,
                          description=pkg.__doc__)))


def run():
    requires, links = requirements('requirements.txt')

    meta = dict(
        name='pulsar-odm',
        author="Luca Sbardella",
        author_email="luca@quantmind.com",
        url="https://github.com/quantmind/pulsar-odm",
        zip_safe=False,
        license='BSD',
        long_description=read('README.rst'),
        packages=find_packages(include=('odm', 'odm.*')),
        setup_requires=['pulsar', 'wheel'],
        install_requires=requires,
        dependency_links=links,
        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Topic :: Utilities'])

    setup(**extend(meta, 'odm'))


if __name__ == '__main__':
    command = sys.argv[1] if len(sys.argv) > 1 else None
    if command == 'package_info':
        package_info()
    elif command == 'agile':
        from agile.app import AgileManager
        AgileManager(description='Release manager for pulsar-odm',
                     argv=sys.argv[2:]).start()
    else:
        run()
