Overview
========

Tools to debug a ZODB.

Provide the ``.core.ZODBInfo`` class which is a nice interface to inspect the objects in a ZODB
and the references between them.

The ``scan_blobs`` script scans the blobs directory and prints information about each blob.
It is registered using the ``zopectl.command`` entry-point, so it can be invoked like this:
``bin/instance scan_blobs``.

.. DANGER::

   Do not use in production! This project provides debugging tools only. For safety always use it
   in a copy of the actual DB.


Install
=======

Install it as an egg of a Zope instance.


See also
========

This discussion: `Any way to track down the content object a blobstorage asset belongs to? (RelStorage)`_


.. References:

.. _`Any way to track down the content object a blobstorage asset belongs to? (RelStorage)`: https://community.plone.org/t/any-way-to-track-down-the-content-object-a-blobstorage-asset-belongs-to-relstorage/7191

