# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import io
from setuptools import setup, find_packages


# Helpers
def read(*paths):
    """Read a text file."""
    basedir = os.path.dirname(__file__)
    fullpath = os.path.join(basedir, *paths)
    contents = io.open(fullpath, encoding='utf-8').read().strip()
    return contents


# Prepare
PACKAGE = 'dataflows'
NAME = PACKAGE.replace('_', '-')
INSTALL_REQUIRES = [
    'datapackage>=1.5.0',
    'kvfile>=0.0.6',
    'click',
    'jinja2',
    'awesome-slugify',
    'inquirer',
    'tabulate',
    'tableschema-sql',
]
SPEEDUP_REQUIRES = [
    'plyvel',
]
LINT_REQUIRES = [
    'pylama',
]
TESTS_REQUIRE = [
    'tox',
]
README = read('README.md')
VERSION = read(PACKAGE, 'VERSION')
PACKAGES = find_packages(exclude=['examples', 'tests', '.tox'])

# Run
setup(
    name=NAME,
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    extras_require={
        'develop': LINT_REQUIRES + TESTS_REQUIRE,
        'speedup': SPEEDUP_REQUIRES,
    },
    zip_safe=False,
    long_description=README,
    description='A nifty data processing framework, based on data packages',
    author='Adam Kariv',
    author_email='adam.kariv@gmail.com',
    url='https://github.com/datahq/dataflows',
    license='MIT',
    keywords=[
        'data',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    entry_points={
      'console_scripts': [
        'dataflows = dataflows.cli:cli',
      ]
    },
    dependency_links=[
        'https://github.com/frictionlessdata/datapackage-py/archive/feature/expose-tableschema-infer-options.zip#egg=datapackage-1.3.1alpha'  # Link with version at the end
    ]

)
