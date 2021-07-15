#!/usr/bin/env python

from setuptools import setup, find_packages
from codecs import open
import os.path


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def parse_requirements(filename):
    return [line.strip()
            for line in read(filename).strip().split('\n')
            if line.strip()]


pkg = {}
exec(read('erde/__pkg__.py'), pkg)

ver = {}
exec(read('erde/_version.py'), ver)
ver['write_version']('erde/__version__.py')
exec(read('erde/__version__.py'), pkg)

readme = "" # read('README.md')
requirements = parse_requirements('requirements.txt')

setup(
    author=pkg['__author__'],
    author_email=pkg['__email__'],
    description=pkg['__description__'],
    license=pkg['__license__'],
    long_description="",  # open('README.md').read(),
    name=pkg['__package_name__'],
    url=pkg['__url__'],
    version=pkg['__version__'],
    classifiers=[
        'Topic :: Utilities'
    ],
    packages=find_packages(exclude=['tests.py']),
    entry_points={'console_scripts': []},
    install_requires=requirements,
    tests_require=['pytest']
)
