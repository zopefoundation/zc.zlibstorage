=========
 Changes
=========

1.2.0 (2017-01-20)
==================

- Add support for Python 3.6 and PyPy.

- Test with both ZODB/ZEO 4 and ZODB/ZEO 5.
  Note that ServerZlibStorage cannot be used in a ZODB 5 Connection
  (e.g., client-side, which wouldn't make sense :-]).
  (https://github.com/zopefoundation/zc.zlibstorage/issues/5).

- Close the underlying iterator used by the ``iterator`` wrapper when
  it is closed. (https://github.com/zopefoundation/zc.zlibstorage/issues/4)

1.1.0 (2016-08-03)
==================

- Fixed an incompatibility with ZODB5.  The previously optional and
  ignored version argument to the database ``invalidate`` method is now
  disallowed.

- Drop Python 2.6, 3.2, and 3.3 support. Added Python 3.4 and 3.5 support.

1.0.0 (2015-11-11)
==================

- Python 3 support contributed by Christian Tismer.

0.1.1 (2010-05-26)
==================

- Fixed a packaging bug.

0.1.0 (2010-05-20)
==================

Initial release
