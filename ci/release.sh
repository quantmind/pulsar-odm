#!/usr/bin/env bash

VERSION="$(python setup.py --version)"
echo ${VERSION}

python setup.py pypi --final
python setup.py sdist
twine upload dist/* --username lsbardel --password ${PYPI_PASSWORD}

git push
git tag -am "Release $VERSION [ci skip]" ${VERSION}
git push --tags
