#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-listrak",
    version="1.1.0",
    description="Singer.io tap for extracting data from the Listrak API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_listrak"],
    install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
        "zeep==4.3.1",
        'backoff==2.2.1',
        'pendulum==3.1.0',
        "urllib3==1.26.18",  # Locked version to prevent import errors
        "six==1.16.0"        # Official last release, stable and compatible
    ],
    entry_points="""
    [console_scripts]
    tap-listrak=tap_listrak:main
    """,
    packages=["tap_listrak"],
    package_data = {
        "schemas": ["tap_listrak/schemas/*.json"]
    },
    include_package_data=True,
)
