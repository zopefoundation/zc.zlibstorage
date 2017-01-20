##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
name, version = 'zc.zlibstorage', '1.2.0'

install_requires = [
    'setuptools',
    'ZODB',
    'zope.interface',
]
extras_require = {
    'test': [
        'zope.testing',
        'manuel',
        'ZEO[test]',
    ]
}

entry_points = """
"""

from setuptools import setup
import os


def read(filename):
    with open(filename) as f:
        return f.read()


readme = read(os.path.join('src', *name.split('.') + ['README.txt']))
changes = read('CHANGES.rst')
long_description = readme + '\n\n' + changes

setup(
    author='Jim Fulton',
    author_email='jim@zope.com',
    license='ZPL 2.1',

    name=name, version=version,
    long_description=long_description,
    description=long_description.split('\n')[1],
    classifiers=[
        "Programming Language :: Python",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    packages=[name.split('.')[0], name],
    namespace_packages=[name.split('.')[0]],
    package_dir={'': 'src'},
    install_requires=install_requires,
    zip_safe=False,
    entry_points=entry_points,
    include_package_data=True,
    extras_require=extras_require,
    tests_require=extras_require['test'],
    test_suite=name + '.tests.test_suite',
)
