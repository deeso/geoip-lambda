#!/usr/bin/env python
from setuptools import setup, find_packages


DESC ='geoip-lambda acts as a lambda service updating json with Geo IP informaton'
setup(name='geoip-lambda',
      version='1.0',
      description=DESC,
      author='adam pridgen',
      author_email='dso@thecoverofnight.com',
      install_requires=['geoip', 'toml'],
      packages=find_packages('src'),
      package_dir={'': 'src'},
      include_package_data=True,
)
