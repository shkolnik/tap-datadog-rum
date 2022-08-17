#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-datadog-rum",
    version="0.1.0",
    description="Singer.io tap for extracting events from the Datadog Real User Monitoring (RUM) API",
    author="James Shkolnik (js@gusto.com)",
    url="http://github.com/Gusto/tap-datadog-rum",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_datadog_rum"],
    install_requires=[
        "singer-python",
        "datadog-api-client>=2.2",
        "datadog-api-client<3",
        "genson>=1.2.2",
        "genson<2.0",
    ],
    entry_points="""
    [console_scripts]
    tap-datadog-rum=tap_datadog_rum:main
    """,
    packages=["tap_datadog_rum"],
    package_data = {},
    include_package_data=False,
)
