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
import manuel.capture
import manuel.doctest
import manuel.testing
import transaction
import unittest
import zc.zlibstorage
import ZEO.tests.testZEO
import zlib
import ZODB.config
import ZODB.FileStorage
import ZODB.interfaces
import ZODB.MappingStorage
import ZODB.tests.StorageTestBase
import ZODB.tests.testFileStorage
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
    >>> conn.root.a = 1
    >>> transaction.commit()
    >>> conn.root.b = ZODB.blob.Blob('Hi\nworld.\n')
    >>> transaction.commit()
    >>> conn.root.c = conn.root().__class__((i,i) for i in range(100))
    >>> transaction.commit()
    >>> db.close()

Now let's open the database compressed:

    >>> db = ZODB.DB(zc.zlibstorage.ZlibStorage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs')))
    >>> conn = db.open()
    >>> conn.root()['a']
    1
    >>> conn.root()['b'].open().read()
    'Hi\nworld.\n'
    >>> conn.root()['b'] = ZODB.blob.Blob('Hello\nworld.\n')
    >>> transaction.commit()
    >>> db.close()

Having updated the root, it is now compressed.  To see this, we'll
open it as a file storage and inspect the record for object 0:

    >>> storage = ZODB.FileStorage.FileStorage('data.fs')
    >>> data, _ = storage.load('\0'*8)
    >>> data[:2] == '.z'
    True
    >>> zlib.decompress(data[2:])[:50]
    'cpersistent.mapping\nPersistentMapping\nq\x01.}q\x02U\x04data'

The new blob record is uncompressed because it is too small:

    >>> storage.load('\0'*7+'\3')[0]
    'cZODB.blob\nBlob\nq\x01.N.'

Records that we didn't modify remain uncompressed

    >>> storage.load('\0'*7+'\2')[0] # doctest: +ELLIPSIS
    'cpersistent.mapping\nPersistentMapping...


    >>> storage.close()

Let's try packing the file 4 ways:

- using the compressed storage:

    >>> open('data.fs.save', 'wb').write(open('data.fs', 'rb').read())
    >>> db = ZODB.DB(zc.zlibstorage.ZlibStorage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs')))
    >>> db.pack()
    >>> sorted(ZODB.utils.u64(i[0]) for i in record_iter(db.storage))
    [0, 2, 3]
    >>> db.close()

- using the storage in non-compress mode:

    >>> open('data.fs', 'wb').write(open('data.fs.save', 'rb').read())
    >>> db = ZODB.DB(zc.zlibstorage.ZlibStorage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs'),
    ...     compress=False))

    >>> db.pack()
    >>> sorted(ZODB.utils.u64(i[0]) for i in record_iter(db.storage))
    [0, 2, 3]
    >>> db.close()

- using the server storage:

    >>> open('data.fs', 'wb').write(open('data.fs.save', 'rb').read())
    >>> db = ZODB.DB(zc.zlibstorage.ServerZlibStorage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs'),
    ...     compress=False))

    >>> db.pack()
    >>> sorted(ZODB.utils.u64(i[0]) for i in record_iter(db.storage))
    [0, 2, 3]
    >>> db.close()

- using the server storage in non-compress mode:

    >>> open('data.fs', 'wb').write(open('data.fs.save', 'rb').read())
    >>> db = ZODB.DB(zc.zlibstorage.ServerZlibStorage(
    ...     ZODB.FileStorage.FileStorage('data.fs', blob_dir='blobs'),
    ...     compress=False))

    >>> db.pack()
    >>> sorted(ZODB.utils.u64(i[0]) for i in record_iter(db.storage))
    [0, 2, 3]
    >>> db.close()
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

    >>> s = zc.zlibstorage.ZlibStorage(ZODB.MappingStorage.MappingStorage())
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

def dont_double_compress():
    """
    This test is a bit artificial in that we want to make sure we
    don't double compress and we don't want to rely on not double
    compressing simply because doing so would make the pickle smaller.
    So this test is actually testing that we don't compress strings
    that start withe the compressed marker.

    >>> data = '.z'+'x'*80
    >>> store = zc.zlibstorage.ZlibStorage(ZODB.MappingStorage.MappingStorage())
    >>> store._transform(data) == data
    True
    """

def record_iter(store):
    next = None
    while 1:
        oid, tid, data, next = store.record_iternext(next)
        yield oid, tid, data
        if next is None:
            break


class FileStorageZlibTests(ZODB.tests.testFileStorage.FileStorageTests):

    def open(self, **kwargs):
        self._storage = zc.zlibstorage.ZlibStorage(
            ZODB.FileStorage.FileStorage('FileStorageTests.fs',**kwargs))

class FileStorageZlibTestsWithBlobsEnabled(
    ZODB.tests.testFileStorage.FileStorageTests):

    def open(self, **kwargs):
        if 'blob_dir' not in kwargs:
            kwargs = kwargs.copy()
            kwargs['blob_dir'] = 'blobs'
        ZODB.tests.testFileStorage.FileStorageTests.open(self, **kwargs)
        self._storage = zc.zlibstorage.ZlibStorage(self._storage)

class FileStorageZlibRecoveryTest(
    ZODB.tests.testFileStorage.FileStorageRecoveryTest):

    def setUp(self):
        ZODB.tests.StorageTestBase.StorageTestBase.setUp(self)
        self._storage = zc.zlibstorage.ZlibStorage(
            ZODB.FileStorage.FileStorage("Source.fs", create=True))
        self._dst = zc.zlibstorage.ZlibStorage(
            ZODB.FileStorage.FileStorage("Dest.fs", create=True))



class FileStorageZEOZlibTests(ZEO.tests.testZEO.FileStorageTests):
    _expected_interfaces = (
        ('ZODB.interfaces', 'IStorageRestoreable'),
        ('ZODB.interfaces', 'IStorageIteration'),
        ('ZODB.interfaces', 'IStorageUndoable'),
        ('ZODB.interfaces', 'IStorageCurrentRecordIteration'),
        ('ZODB.interfaces', 'IExternalGC'),
        ('ZODB.interfaces', 'IStorage'),
        ('ZODB.interfaces', 'IStorageWrapper'),
        ('zope.interface', 'Interface'),
        )

    def getConfig(self):
        return """\
        %import zc.zlibstorage
        <zlibstorage>
        <filestorage 1>
        path Data.fs
        </filestorage>
        </zlibstorage>
        """

class FileStorageClientZlibTests(FileStorageZEOZlibTests):

    def getConfig(self):
        return """\
        %import zc.zlibstorage
        <serverzlibstorage>
        <filestorage 1>
        path Data.fs
        </filestorage>
        </serverzlibstorage>
        """

    def _wrap_client(self, client):
        return zc.zlibstorage.ZlibStorage(client)

def test_suite():
    suite = unittest.TestSuite()
    for class_ in (
        FileStorageZlibTests,
        FileStorageZlibTestsWithBlobsEnabled,
        FileStorageZlibRecoveryTest,
        FileStorageZEOZlibTests,
        FileStorageClientZlibTests,
        ):
        s = unittest.makeSuite(class_, "check")
        s.layer = ZODB.tests.util.MininalTestLayer(
            'zlibstoragetests.%s' % class_.__name__)
        suite.addTest(s)

    # The conflict resolution and blob tests don't exercise proper
    # plumbing for libstorage because the sample data they use
    # compresses to larger than the original.  Run the tests again
    # after monkey patching zlibstorage to compress everything.

    class ZLibHackLayer:

        orig = [zc.zlibstorage.compress] # []s hide the function :)

        @classmethod
        def setUp(self):
            zc.zlibstorage.transform = (
                lambda data: data and ('.z'+zlib.compress(data)) or data
                )

        @classmethod
        def tearDown(self):
            [zc.zlibstorage.compress] = self.orig

    s = unittest.makeSuite(FileStorageZlibTestsWithBlobsEnabled, "check")
    s.layer = ZLibHackLayer
    suite.addTest(s)

    suite.addTest(doctest.DocTestSuite(
        setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown
        ))
    suite.addTest(manuel.testing.TestSuite(
        manuel.doctest.Manuel() + manuel.capture.Manuel(),
        'README.txt',
        setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown
        ))
    return suite

