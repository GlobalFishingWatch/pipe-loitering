#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name='loitering',
    version='4.1.2',
    packages=find_packages(exclude=['test*.*', 'tests']),
)

