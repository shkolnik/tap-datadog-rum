#!/usr/bin/env python
from setuptools import setup

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="tap-datadog-rum",
    version="0.7.1",
    description="Singer.io tap for extracting events from the Datadog Real User Monitoring (RUM) API",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="James Shkolnik (js@gusto.com)",
    url="http://github.com/shkolnik/tap-datadog-rum",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_datadog_rum"],
    install_requires=[
        "singer-python",
        "datadog-api-client>=2.2",
        "datadog-api-client<3",
    ],
    entry_points="""
    [console_scripts]
    tap-datadog-rum=tap_datadog_rum:main
    """,
    packages=["tap_datadog_rum"],
    package_data = {},
    include_package_data=False,
)
