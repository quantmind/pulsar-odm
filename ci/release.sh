#!/usr/bin/env bash

set -e


codecov

if [[ $TRAVIS_BRANCH == 'release' ]]; then
    VERSION="$(python setup.py --version)"
    echo ${VERSION}
    echo PyPI source release
    #
    python setup.py pypi --final
    python setup.py sdist
    twine upload dist/* --username lsbardel --password ${PYPI_PASSWORD}
    #
    echo Create a new github tag
    git push
    git tag -am "Release $VERSION [ci skip]" ${VERSION}
    git push --tags
fi
