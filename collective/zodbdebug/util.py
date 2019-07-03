# coding=utf8
from functools import wraps
import itertools
import logging
import os
import sys
import binascii

_MARKER = object()


def pairwise(iterable, marker=None):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    (a, b) = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, itertools.chain(b, [marker]))


def mkdirp(caminho):
    if not os.path.exists(caminho):
        os.makedirs(caminho)


def cache_in_obj(f):
    u"""Cache decorator for functions with takes only one `obj` argument.

    The result is cached on a non-persistent attribute of the object.
    """
    cache_attr = '_v_cached_' + f.__name__

    @wraps(f)
    def new_f(obj):
        result = getattr(obj, cache_attr, _MARKER)
        if result is _MARKER:
            result = f(obj)

            try:
                setattr(obj, cache_attr, result)
            except AttributeError:
                pass

        return result

    return new_f


def cache_get_oid_path(f):

    @wraps(f)
    def new_f(self, oid, preceding_path=()):
        cache = getattr(self, '_oid_paths_cache', None)
        if cache is None:
            cache = self._oid_paths_cache = {}

        oid_repr = self.oid_or_repr_to_repr(oid)
        path = cache.get(oid)
        if path:
            if set(path).isdisjoint(preceding_path):
                self._logger.debug('OID Path Cache HIT! oid = {}'.format(oid_repr))
                return path
            else:
                self._logger.warning(
                    'Retrieved bad OID path from cache. '
                    'oid={}, preceding_path={}, retrieved_path={}'.format(
                        oid_repr,
                        '/'.join(self.oid_or_repr_to_repr(o) for o in preceding_path),
                        '/'.join(self.oid_or_repr_to_repr(o) for o in path),
                    )
                )
        else:
            self._logger.debug('OID Path Cache MISS! oid = {}'.format(oid_repr))

        path = f(self, oid, preceding_path)
        for i in xrange(len(path)):
            # Do not overwrite existing entries in the cache.
            cache.setdefault(path[i], path[i:])

        return path

    return new_f


def setup_logging(level=logging.INFO):
    u"""Setup logging for use in CLI scripts."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(levelname)-7s [%(asctime)s] %(name)s: %(message)s',
        '%H:%M:%S'
    )
    handler.setFormatter(formatter)
    root_logger.handlers = [handler]
    logging.getLogger('requests').setLevel(logging.WARNING)


def get_arguments(argv=None):
    u"""Get the arguments passed to the script in the command line.

    When the scripts are executed via the values in the args list will contain other stuff besides
    the actual arguments to the script. This function return only the relevant arguments.

    The result of this function can be passed to the `docpt` function.

    Arguments:
    argv (Optional[Sequence[str]]) -- Sequence of arguments. Defaults to `sys.argv`.

    Return (Sequence[str]) -- Arguments passed to the script.
    """
    if argv is None:
        argv = sys.argv

    return list(itertools.dropwhile(lambda i: i != '-c', argv))[2:]
