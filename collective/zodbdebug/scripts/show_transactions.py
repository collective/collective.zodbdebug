# coding=utf8
u"""Print information about the ZODB transactions.

Usage:
  show_transactions [options] <start> <count>

Arguments:
  Arguments are interpreted as if the transactions are represented by a list, where 0 is the index
  of the most recent transaction. The selected transactions will be transactions[start:start+count].

Examples:
  Most recent transaction: show_transactions 0 1
  Five transactions starting at the third most recent transaction: show_transactions 2 5

Options:
  -h, --help                            Print this message.
"""
from ..core import ZODBInfo
from ..util import get_arguments
from ..util import setup_logging
from docopt import docopt
import itertools
import logging


log = logging.getLogger(__name__)


def main(app, cmd_args):
    arguments = docopt(__doc__, argv=get_arguments(cmd_args))  # noqa
    setup_logging()

    start = int(arguments['<start>'])
    count = int(arguments['<count>'])

    diagnose_transactions(app, start, count)
    log.info('Finish!')


def diagnose_transactions(app, start, count):
    zodb_info = ZODBInfo(app._p_jar)
    zodb_info.build_reference_maps()

    transactions = reversed(list(zodb_info.iter_oids_modified_by_each_transaction()))
    transactions = itertools.islice(transactions, start, start + count)
    for (i, oids) in enumerate(transactions):
        print 'Transaction {}'.format(start + i)
        print 'Number of modified objects: {}'.format(len(oids))
        for oid in oids:
            print
            print zodb_info.get_oid_info(oid)
        print '-' * 80


def _str_to_int_or_none(s):
    return None if ((not s) or (s.lower() == 'none')) else int(s)
