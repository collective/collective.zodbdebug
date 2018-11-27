# coding=utf8
u"""Scan the blobs directory and print information about each blob.

Usage:
  zodbdebug_blobs [options]

Options:
  -h, --help                            Print this message.
"""
from docopt import docopt
from collective.zodbdebug.core import ZODBInfo
from collective.zodbdebug.util import setup_logging
import hashlib
import logging
import math
import os


PLONE_ADMIN_USER = 'ploneadmin'

app = app  # noqa
log = logging.getLogger('diagnose_blobs')


def hash_file(path):
    with open(path, 'r') as f:
        data = f.read(1024)
    return '({},{})'.format(hashlib.md5(data).hexdigest(), os.path.getsize(path))


def diagnose_blobs(app):
    zodb_info = ZODBInfo(app._p_jar)
    zodb_info.build_reference_maps()

    blob_paths = sorted(zodb_info.iter_blob_paths())
    num_blobs = len(blob_paths)
    print 'Number of blobs: {}'.format(num_blobs)
    print

    for (i, path) in enumerate(blob_paths):
        num = i + 1
        percent = int(math.ceil((float(num) / num_blobs) * 100.))
        log.info('Processing blob {} of {} ({}%)...'.format(num, num_blobs, percent))
        print 'Blob path: ' + path
        print 'Blob hash: ' + hash_file(path)
        oid = zodb_info.blob_path_to_oid(path)
        print zodb_info.get_oid_info(oid)
        print


def main(app, cmd_args):
    arguments = docopt(__doc__, argv=cmd_args)  # noqa
    setup_logging()
    diagnose_blobs(app)
    log.info('Finish!')


if 'app' in locals():
    main(app)
