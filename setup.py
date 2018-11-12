#!/usr/bin/env python

from setuptools import setup

setup(name='tap-listrak',
      version='2.0.7',
      description='Singer.io tap for extracting data from the Listrak API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_listrak'],
      install_requires=[
          'backoff==1.3.2',
          'requests==2.20.0',
          'pendulum==2.0.3',
          'singer-python==5.2.0'
      ],
      entry_points='''
          [console_scripts]
          tap-listrak=tap_listrak:main
      ''',
      packages=['tap_listrak'],
      include_package_data=True,
)
