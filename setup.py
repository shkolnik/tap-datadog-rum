#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-datadog-rum",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_datadog_rum"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-datadog-rum=tap_datadog_rum:main
    """,
    packages=["tap_datadog_rum"],
    package_data = {
        "schemas": ["tap_datadog_rum/schemas/*.json"]
    },
    include_package_data=True,
)
