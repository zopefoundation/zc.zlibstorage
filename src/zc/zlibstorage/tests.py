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
from zope.testing import setupstack

import doctest
import transaction
import unittest
import zc.zlibstorage
import zlib
import ZODB.config
import ZODB.FileStorage
import ZODB.interfaces
import ZODB.MappingStorage
import ZODB.utils
import zope.interface.verify


def test_config():
    r"""

To configure a zlibstorage, import zc.zlibstorage and use the
zlibstorage tag:

    >>> config = '''
    ...     %import zc.zlibstorage
    ...     <zodb>
    ...         <zlibstorage>
    ...             <filestorage>
    ...                 path data.fs
    ...                 blob-dir blobs
    ...             </filestorage>
    ...         </zlibstorage>
    ...     </zodb>
    ... '''
    >>> db = ZODB.config.databaseFromString(config)

    >>> conn = db.open()
    >>> conn.root()['a'] = 1
    >>> transaction.commit()
    >>> conn.root()['b'] = ZODB.blob.Blob('Hi\nworld.\n')
    >>> transaction.commit()

    >>> db.close()

    >>> db = ZODB.config.databaseFromString(config)
    >>> conn = db.open()
    >>> conn.root()['a']
    1
    >>> conn.root()['b'].open().read()
    'Hi\nworld.\n'
    >>> db.close()

After putting some data in, the records will be compressed, unless
doing so would make them bigger:

    >>> for t in ZODB.FileStorage.FileIterator('data.fs'):
    ...     for r in t:
    ...         data = r.data
    ...         if r.data[:2] != '.z':
    ...             if len(zlib.compress(data))+2 < len(data):
    ...                 print 'oops', `r.oid`
    ...         else: _ = zlib.decompress(data[2:])
    """

def test_config_no_compress():
    r"""

You can disable compression.

    >>> config = '''
    ...     %import zc.zlibstorage
    ...     <zodb>
    ...         <zlibstorage>
    ...             compress no
    ...             <filestorage>
    ...                 path data.fs
    ...                 blob-dir blobs
    ...             </filestorage>
    ...         </zlibstorage>
    ...     </zodb>
    ... '''
    >>> db = ZODB.config.databaseFromString(config)

    >>> conn = db.open()
    >>> conn.root()['a'] = 1
    >>> transaction.commit()
    >>> conn.root()['b'] = ZODB.blob.Blob('Hi\nworld.\n')
    >>> transaction.commit()

    >>> db.close()

Since we didn't compress, we can open the storage using a plain file storage:

    >>> db = ZODB.DB(ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs'))
    >>> conn = db.open()
    >>> conn.root()['a']
    1
    >>> conn.root()['b'].open().read()
    'Hi\nworld.\n'
    >>> db.close()
    """

def test_mixed_compressed_and_uncompressed_and_packing():
    r"""
We can deal with a mixture of compressed and uncompressed data.

First, we'll create an existing file storage:

    >>> db = ZODB.DB(ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs'))
    >>> conn = db.open()
    >>> conn.root()['a'] = 1
    >>> transaction.commit()
    >>> conn.root()['b'] = ZODB.blob.Blob('Hi\nworld.\n')
    >>> transaction.commit()
    >>> conn.root()['c'] = conn.root().__class__()
    >>> conn.root()['c']['a'] = conn.root().__class__()
    >>> transaction.commit()
    >>> db.close()

Now let's open the database compressed:

    >>> db = ZODB.DB(zc.zlibstorage.Storage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs')))
    >>> conn = db.open()
    >>> conn.root()['a']
    1
    >>> conn.root()['b'].open().read()
    'Hi\nworld.\n'
    >>> del conn.root()['b']
    >>> transaction.commit()
    >>> db.close()

Having updated the root, it is now compressed.  To see this, we'll
open it as a file storage and inspect the record for object 0:

    >>> s = ZODB.FileStorage.FileStorage('data.fs')
    >>> data, _ = s.load('\0'*8)
    >>> data[:2] == '.z'
    True
    >>> zlib.decompress(data[2:])[:50]
    'cpersistent.mapping\nPersistentMapping\nq\x01.}q\x02U\x04data'

The blob record is still uncompressed:

    >>> s.load('\0'*7+'\1')

    >>> s.close()

Let's try packing the file 2 ways:

- using the compressed storage:

    >>> open('data.fs.save', 'wb').write(open('data.fs', 'rb').read())
    >>> db = ZODB.DB(zc.zlibstorage.Storage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs')))
    >>> db.pack()
    >>> sorted(ZODB.utils.u64(i[0]) for i in record_iter(db.storage))

- and using the storage in non-compress mode:

    >>> open('data.fs.save', 'wb').write(open('data.fs', 'rb').read())
    >>> db = ZODB.DB(zc.zlibstorage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs'),
    ...     compress=False))
    >>> db.pack()
    >>> sorted(ZODB.utils.u64(i[0]) for i in record_iter(db.storage))
    """

class Dummy:

    def invalidateCache(self):
        print 'invalidateCache called'

    def invalidate(self, *args):
        print 'invalidate', args

    def references(self, record, oids=None):
        if oids is None:
            oids = []
        oids.extend(record.decode('hex').split())
        return oids

    def transform_record_data(self, data):
        return data.encode('hex')

    def untransform_record_data(self, data):
        return data.decode('hex')


def test_wrapping():
    """
Make sure the wrapping methods do what's expected.

    >>> s = zc.zlibstorage.Storage(ZODB.MappingStorage.MappingStorage())
    >>> zope.interface.verify.verifyObject(ZODB.interfaces.IStorageWrapper, s)
    True

    >>> s.registerDB(Dummy())
    >>> s.invalidateCache()
    invalidateCache called

    >>> s.invalidate('1', range(3), '')
    invalidate ('1', [0, 1, 2], '')

    >>> data = ' '.join(map(str, range(9)))
    >>> transformed = s.transform_record_data(data)
    >>> transformed == '.z'+zlib.compress(data.encode('hex'))
    True

    >>> s.untransform_record_data(transformed) == data
    True

    >>> s.references(transformed)
    ['0', '1', '2', '3', '4', '5', '6', '7', '8']

    >>> l = range(3)
    >>> s.references(transformed, l)
    [0, 1, 2, '0', '1', '2', '3', '4', '5', '6', '7', '8']

    >>> l
    [0, 1, 2, '0', '1', '2', '3', '4', '5', '6', '7', '8']

If the data are small or otherwise not compressable, it is left as is:

    >>> data = ' '.join(map(str, range(2)))
    >>> transformed = s.transform_record_data(data)
    >>> transformed == '.z'+zlib.compress(data.encode('hex'))
    False

    >>> transformed == data.encode('hex')
    True

    >>> s.untransform_record_data(transformed) == data
    True

    >>> s.references(transformed)
    ['0', '1']

    >>> l = range(3)
    >>> s.references(transformed, l)
    [0, 1, 2, '0', '1']

    >>> l
    [0, 1, 2, '0', '1']


    """

def record_iter(store):
    next = None
    while 1:
        oid, tid, data, next = storage.record_iternext(next)
        yield oid, tid, data
        if next is None:
            break


def test_suite():
    return unittest.TestSuite((
        doctest.DocTestSuite(
            setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown),
        ))
