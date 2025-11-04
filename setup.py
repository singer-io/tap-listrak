#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-listrak",
    version="1.1.1",
    description="Singer.io tap for extracting data from the Listrak API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_listrak"],
    install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
        "zeep",
        'backoff==2.2.1',
        'pendulum==3.1.0'
    ],
    entry_points="""
    [console_scripts]
    tap-listrak=tap_listrak:main
    """,
    packages=find_packages(),
    package_data = {
        "tap_listrak/schemas": ["*.json"]
    },
    include_package_data=True,
)
