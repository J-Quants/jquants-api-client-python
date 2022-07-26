"""Setup for J-Quants API client.
"""
from __future__ import print_function

import io
import os
import sys

from setuptools import setup, find_packages

if sys.version_info.major < 3:
    print("jquants-api-client requires python version 3", file=sys.stderr)
    sys.exit(1)

require_packages = [
    "requests",
    "pandas",
    "python-dateutil",
]

package_root = os.path.abspath(os.path.dirname(__file__))
readme_filename = os.path.join(package_root, "README.md")
with io.open(readme_filename, encoding="utf-8") as readme_file:
    readme = readme_file.read()

setup(
    name="jquants-api-client-python",
    version="0.1.0",
    description="J-Quants API Client Library",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Alpaca",
    author_email="oss@alpaca.markets",
    url="https://github.com/J-Quants/jquants-api-client-python",
    install_requires=require_packages,
    python_requires=">=3",
    packages=find_packages(where='src', exclude=['tests']),
    license="Apache 2.0",
    keywords="jquants api client",
)
