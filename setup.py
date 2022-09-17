#!/usr/bin/env python
import os
import sys

from setuptools import setup
from setuptools.command.install import install

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="codeli",
    version="0.0.2",
    description="This package runs common data engineering tasks as a configuration.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Datafellows/codeli",
    project_urls={
        'Source': 'https://github.com/Datafellows/codeli',
        'Issues': 'https://github.com/Datafellows/codeli/issues'
    },
    author="Data Fellows",
    author_email="codeli@datafellows.nl",
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3"
    ],
    keywords='codeli',
    package_dir={'': 'src'},
    packages=['codeli'],
    python_requires='>=3.6'
)