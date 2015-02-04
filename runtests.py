#!/usr/bin/env python
import sys
import os
from multiprocessing import current_process


def run(**params):
    from pulsar.apps.test import TestSuite
    from pulsar.apps.test.plugins import bench, profile

    args = params.get('argv', sys.argv)
    if '--coveralls' in args:
        import pulsar
        from pulsar.utils.path import Path
        from pulsar.apps.test.cov import coveralls

        path = Path(__file__)
        repo_token = None
        strip_dirs = [Path(pulsar.__file__).parent.parent, os.getcwd()]
        if os.path.isfile('.coveralls-repo-token'):
            with open('.coveralls-repo-token') as f:
                repo_token = f.read().strip()
        code = coveralls(strip_dirs=strip_dirs,
                         repo_token=repo_token)
        sys.exit(0)
    # Run the test suite
    #
    TestSuite(description='ODM Asynchronous test suite',
              modules=['tests'],
              plugins=(bench.BenchMark(),
                       profile.Profile()),
              pidfile='test.pid',
              **params).start()


if __name__ == '__main__':
    run()
