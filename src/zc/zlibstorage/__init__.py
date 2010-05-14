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
import zlib
import ZODB.interfaces
import zope.interface

class Storage(object):

    zope.interface.implements(ZODB.interfaces.IStorageWrapper)

    def __init__(self, base, compress=True):
        self.base = base
        self.compress = compress

        for name in (
            'close', 'getName', 'getSize', 'history', 'isReadOnly',
            'lastTransaction', 'new_oid', 'sortKey',
            'tpc_abort', 'tpc_begin', 'tpc_finish', 'tpc_vote',
            'loadBlob', 'openCommittedBlobFile', 'temporaryDirectory',
            'supportsUndo', 'undo', 'undoLog', 'undoInfo',
            ):
            v = getattr(base, name, None)
            if v is not None:
                setattr(self, name, v)

        zope.interface.directlyProvides(self, zope.interface.providedBy(base))

    def _transform(self, data):
        if self.compress:
            compressed = '.z'+zlib.compress(data)
            if len(compressed) < len(data):
                return compressed
        return data

    def _untransform(self, data):
        if data[:2] == '.z':
            return zlib.decompress(data[2:])
        return data

    def __len__(self):
        return len(self.base)

    def load(self, oid, version=''):
        data, serial = self.base.load(oid, version)
        return self._untransform(data), serial

    def loadBefore(self, oid, tid):
        r = self.base.loadBefore(oid, tid)
        if r is not None:
            data, serial, after = r
            return self._untransform(data), serial, after
        else:
            return r

    def loadSerial(self, oid, serial):
        return self._untransform(self.base.loadSerial(oid, serial))

    def pack(self, pack_time, referencesf):
        _untransform = self._untransform
        def refs(p, oids=None):
            return referencesf(_untransform(p), oids)

    def registerDB(self, db):
        self.db = db

    def store(self, oid, serial, data, version, transaction):
        if self.compress:
            data = self._transform(data)
        return self.base.store(oid, serial, data, version, transaction)

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        if self.compress:
            data = self._transform(data)
        return self.base.restore(
            oid, serial, data, version, prev_txn, transaction)

    def iterator(self, start=None, stop=None):
        for t in self.base.iterator(start, end):
            yield Transaction(t)

    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        if self.compress:
            data = self._transform(data)
        return self.base.storeBlob(oid, oldserial, data, blobfilename, version,
                                   transaction)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn,
                    transaction):
        if self.compress:
            data = self._transform(data)
        return self.base.restoreBlob(oid, serial, data, blobfilename, prev_txn,
                                     transaction)

    def invalidateCache(self):
        return self.db.invalidateCache()

    def invalidate(self, transaction_id, oids, version=''):
        return self.db.invalidate(transaction_id, oids, version)

    def references(self, record, oids=None):
        return self.db.references(self._untransform(record), oids)

    def transform_record_data(self, data):
        return self._transform(self.db.transform_record_data(data))

    def untransform_record_data(self, data):
        return self.db.untransform_record_data(self._untransform(data))


class Transaction(object):

    def __init__(self, store, trans):
        self.__store = store
        self.__trans = trans

    def __iter__(self):
        for r in self.__trans:
            if r.data:
                r.data = self.__store._untransform(r.data)
            yield r

    def __getattr__(self, name):
        return getattr(self.__trans)

class ZConfig:

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        base = self.config.base.open()
        compress = self.config.compress
        if compress is None:
            compress = True
        return Storage(base, compress)
