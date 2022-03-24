#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name='loitering',
    version='1.1.1',
    packages=find_packages(exclude=['test*.*', 'tests']),
)

