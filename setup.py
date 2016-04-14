from setuptools import setup, find_packages

import odm_config as config


def run():
    requires, links = config.requirements('requirements.txt')

    meta = dict(
        name='pulsar-odm',
        author="Luca Sbardella",
        author_email="luca@quantmind.com",
        url="https://github.com/quantmind/pulsar-odm",
        zip_safe=False,
        license='BSD',
        long_description=config.read('README.rst'),
        packages=find_packages(include=('odm', 'odm.*')),
        setup_requires=['pulsar'],
        install_requires=requires,
        dependency_links=links,
        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.4',
            'Topic :: Utilities'])

    setup(**config.setup(meta, 'odm'))


if __name__ == '__main__':
    run()
