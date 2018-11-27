# coding=utf8
from .config import PACKAGE_NAME
from .util import cache_get_oid_path
from .util import mkdirp
from .util import pairwise
from ZODB.POSException import POSKeyError
from ZODB.serialize import referencesf
from ZODB.utils import oid_repr
from ZODB.utils import repr_to_oid
from ZODB.utils import tid_repr
from logging import getLogger
from plone.memoize import instance
from rbco.caseclasses import case
import os
import walkdir


class ZODBInfo(object):
    u"""Provide a better interface to analyze a ZODB.

    The main purpose of this class is to provide information about the references between objects.
    To achieve this a reference map must be calculated. Since it is a slow process it must be done
    explicitly, by calling the `build_reference_maps()` method. Not all methods of this class needs
    this reference map. If the map is needed by a method and it is not built yet, then an exception
    is raised.

    Getting the connection:
        To create an instance of this class a connection object is needed. This can be obtained from
        any persistent object like this: `context._p_jar`.

    WARNING: This class main purpose is to be able to analyze a ZODB. Since some operations may be
        slow, results are heavily cached. This means that if the ZODB is modified stale information
        may be provided by this class. The safest thing to do in this case is to discard the
        instance and create a new one.

    OID and OID representation:
        An OID is a sequence of bytes, represented by an instance of `str`. It can represented
        by a text string (type also `str`), which is called the "OID representation", "repr" or
        "oid_repr". This class contain methods to convert between them. Public methods of this class
        accepts both OIDs and OID representations (auto-detection). Usually OIDs are preferred when
        returning values.

    Concept of OID path:
        An OID path is a sequence of OIDs forming a chain of references.

        Example: suppose we have an structure in the Portal like this:

        app
            plone
                folder
                    subfolder
                        item

        Then the OID path for `oid(Item)` will be something like
        `[oid(item), oid(subfolder), oid(folder), oid(plone_site), oid(app)]`.

        Keep in mind that normally other relationships between objects exists, besides the
        containment relationship (i.e "parent -> child"). This means that multiple paths can exist
        for a given OID. This class tries its best to build "good" OID paths, prefering more
        structural relationships, such as the containment one.
    """

    _EMPTY_FROZENSET = frozenset()
    _EMPTY_TUPLE = tuple()

    def __init__(self, connection):
        self.connection = connection
        self._oids = None
        self._reference_map = None
        self._back_reference_map = None

    @property
    def _logger(self):
        logger = getattr(self, '_logger_instance', None)
        if not logger:
            logger = self._logger_instance = getLogger(__name__ + '.' + type(self).__name__)
        return logger

    @property
    def storage(self):
        u"""(ZODB.interfaces.IStorage) Storage."""
        return self.connection.db().storage

    # OID and OID repr -----------------------------------------------------------------------------

    @instance.memoizedproperty
    def root_oid(self):
        return self.connection.root._root._p_oid

    def repr_to_oid(self, oid_repr):
        return repr_to_oid(oid_repr)

    def oid_to_repr(self, oid):
        return oid_repr(oid)

    def oid_or_repr_to_oid(self, oid_or_repr):
        return self.repr_to_oid(oid_or_repr) if oid_or_repr.startswith('0x') else oid_or_repr

    def oid_or_repr_to_repr(self, oid_or_repr):
        return oid_or_repr if oid_or_repr.startswith('0x') else self.oid_to_repr(oid_or_repr)

    # Reading objects and its main properties ------------------------------------------------------

    # Do not use cache decorators, let ZODB do its caching.
    def get_obj(self, oid):
        u"""Get the object from its `oid'."""
        oid = self.oid_or_repr_to_oid(oid)
        obj = self.connection.get(oid)
        obj._p_activate()
        return obj

    @instance.memoize
    def get_obj_as_str(self, oid):
        try:
            return str(self.get_obj(oid))
        except Exception:
            return '<error>'

    @instance.memoize
    def get_physical_path(self, oid):
        try:
            return self.get_obj(oid).getPhysicalPath()
        except Exception:
            return None

    @instance.memoize
    def get_id(self, oid):
        obj = self.get_obj(oid)

        if oid == self.root_oid:
            return 'Root'

        getId = getattr(obj, 'getId', None)
        if getId:
            try:
                return getId()
            except:  # noqa
                pass
        return getattr(obj, 'id', None)

    @instance.memoize
    def get_attr_name(self, oid, parent_oid):
        oid = self.oid_or_repr_to_oid(oid)
        obj = self.get_obj(oid)
        parent = self.get_obj(parent_oid)
        names_and_values = ((name, getattr(parent, name, None)) for name in dir(parent))
        return next((name for (name, value) in names_and_values if value is obj), None)

    @instance.memoize
    def get_id_or_attr_name(self, oid, parent_oid=None):
        identifier = self.get_id(oid)
        if identifier:
            return identifier

        return self.get_attr_name(oid, parent_oid) if parent_oid else None

    # References -----------------------------------------------------------------------------------

    @property
    def oids(self):
        u"""(Set[str]) Set of all OIDs."""
        if not self._oids:
            raise RuntimeError(u'Reference map is not built. Call `build_reference_maps()`.')
        return self._oids

    @property
    def reference_map(self):
        u"""(Mapping[str, Set[str]]) Mapping from OID to set of referenced oids."""
        if not self._reference_map:
            raise RuntimeError(u'Reference map is not built. Call `build_reference_maps()`.')
        return self._reference_map

    @property
    def back_reference_map(self):
        u"""(Mapping[str, Set[str]]) Mapping from OID to set of oids that references it."""
        if not self._back_reference_map:
            raise RuntimeError(
                u'Reference map is not built. Call `build_back_reference_map()`.'
            )
        return self._back_reference_map

    def build_reference_maps(self):
        u"""Build the forward and back reference maps for the ZODB.

        Looks in every record of every transaction. Results are available in the following
        properties: `oids`, `reference_map`, `back_reference_map`.
        """
        self._logger.info('build_reference_maps: Entered.')

        if self._reference_map or self._back_reference_map:
            raise RuntimeError('Found existing reference map!')

        cache_path = self._get_reference_cache_path()
        self._logger.info('build_reference_maps: Cache file path is {}'.format(cache_path))

        if os.path.exists(cache_path):
            self._logger.info('build_reference_maps: Loading reference maps from file...')
            self._load_reference_cache(cache_path)
        else:
            self._logger.info('build_reference_maps: Cache file not found.')
            self._logger.info('build_reference_maps: Building maps from scratch...')
            self._build_reference_maps_from_scratch()

            self._logger.info('build_reference_maps: Storing reference cache to file...')
            self._store_reference_cache(cache_path)

        self._logger.info('build_reference_maps: Done!')
        self._logger.info(
            'build_reference_maps: len(self._reference_map) == {}'.format(len(self._reference_map))
        )
        self._logger.info(
            'build_reference_maps: len(self._back_reference_map) == {}'.format(
                len(self._back_reference_map))
        )
        self._logger.info(
            'build_reference_maps: len(self.oids) == {}'.format(len(self.oids))
        )

    def get_references(self, oid):
        u"""Get the OIDs refereced by the given `oid`.

        Arguments:
        oid (str) -- OID or OID representation.

        Return (Set[str])
        """
        oid = self.oid_or_repr_to_oid(oid)
        return self.reference_map.get(oid, self._EMPTY_FROZENSET)

    def get_back_references(self, oid):
        u"""Get the OIDs which references the given `oid`.

        Arguments:
        oid (str) -- OID or OID representation.

        Return (Set[str])
        """
        oid = self.oid_or_repr_to_oid(oid)
        return self.back_reference_map.get(oid, self._EMPTY_FROZENSET)

    @instance.memoize
    def get_identified_back_references(self, oid):
        u"""Get the OIDs which references the given `oid` together with the attribute name and
        score.

        Arguments:
        oid (str) -- OID or OID representation.

        Return (Sequence[Tuple[str, str, int]]): Sequence of `(oid, id_or_attr_name, score)` tuples.
        """
        return [
            (
                br,
                self.get_attr_name(oid, parent_oid=br),
                self._get_reference_score(source=br, target=oid),
            )
            for br
            in self.get_back_references(oid)
        ]

    @cache_get_oid_path
    def get_oid_path(self, oid, preceding_path=()):
        """Given an `oid` return an OID path.

        An OID path is a sequence of OIDs forming a chain of references. See the class docstring
        for more details.

        Arguments:
        oid (str) -- OID or OID representation.

        Return (Tuple[str])
        """
        self._logger.debug('get_oid_path({}, {}): Started!'.format(
            self.oid_or_repr_to_repr(oid),
            '/'.join(self.oid_or_repr_to_repr(o) for o in preceding_path),
        ))

        oid = self.oid_or_repr_to_oid(oid)

        path = (oid,)
        preceding_path += path

        bref = self._get_best_back_reference(oid, forbidden=preceding_path)

        if not bref:
            return path

        return (
            path if (not bref)
            else (path + self.get_oid_path(bref, preceding_path=preceding_path))
        )

    @instance.memoize
    def get_id_path(self, oid):
        u"""Given an `oid` return an ID path.

        An ID path is equivalente to an OID path, but OIDs are replaced by the IDs of the
        corresponding objects or the name of attribute in the parent object which holds the
        reference.

        Arguments:
        oid (str) -- OID or OID representation.

        Return (Tuple[str])
        """
        oid = self.oid_or_repr_to_oid(oid)
        return self._oid_path_to_id_path(self.get_oid_path(oid))

    # OIDInfo --------------------------------------------------------------------------------------

    @instance.memoize
    def get_oid_info(self, oid):
        u"""Try to get info about an object from its `oid`.

        Arguments:
        oid (str) -- OID or OID representation.

        Return (OIDInfo)
        """
        oid = self.oid_or_repr_to_oid(oid)
        info = OIDInfo(oid=self.oid_to_repr(oid))
        try:
            info.obj = self.get_obj_as_str(oid)[:50]
            info.oid_path = self.get_oid_path(oid)
            info.id_path = self.get_id_path(oid)
            info.id = self.get_id(oid)
            info.path = self.get_physical_path(oid)
        except POSKeyError:
            pass

        return info

    # Transaction utilities -----------------------------------------------------------------------

    def iter_oids_modified_by_each_transaction(self):
        """Return an iterator of sequences of OIDs modified in each transaction in chronological
        order.
        """
        return ([r.oid for r in t] for t in self.storage.iterator())

    def get_oids_modified_by_last_transaction(self):
        """Return a sequence of OIDs modified by the last transaction."""
        return list(self.iter_oids_modified_by_each_transaction())[-1]

    # Blobs ----------------------------------------------------------------------------------------

    def blob_path_to_oid(self, path):
        return self.storage.fshelper.getOIDForPath(os.path.dirname(path))

    def iter_blob_paths(self):
        for path in walkdir.file_paths(walkdir.filtered_walk(
            self.storage.fshelper.base_dir,
            included_files=['*.blob'],
        )):
            yield path

    # Internal -------------------------------------------------------------------------------------

    @instance.memoize
    def _get_best_back_reference(self, target, forbidden=()):

        def sort_key(back_reference):
            return self._get_reference_score(
                source=back_reference,
                target=target,
            )

        back_references = (br for br in self.get_back_references(target) if br not in forbidden)
        sorted_back_references = sorted(back_references, key=sort_key)

        return sorted_back_references[0] if sorted_back_references else None

    @instance.memoize
    def _get_reference_score(self, source, target, allowed_look_ahead_depth=3):
        u"""Calculate a score for a reference.

        The score is an attempt to classify references to a target object in order
        to choose the most likely to be in the ideal OID path, i.e an OID path consisting
        of a chain of parent/child relationships (see class docstring for more info).

        Return (int): Number from 0 to 100, lower is better.
        """

        # WARNING: The code bellow is very fragile regarding the results it produces in achieving
        # optimal OID paths. It is based in empirical testing. Don't try to change without
        # testing the results on a large data set.
        #
        # The main point is try to avoid paths containing zope.intid related things, since it is an
        # index containing references to everything. On the other hand references of things related
        # to BTrees are good.

        # Avoid bidirectional references, i.e z3c.relationfield.relation.RelationValue objects.
        if source in self.get_references(target):
            return 90

        # Prefer things with IDs, but avoid bad IDs.
        identifier = self.get_id(source)
        if identifier:
            if identifier == 'ldapauth':
                return 20
            elif identifier == 'IIntIds':
                return 60
            else:
                return 10

        # Avoid things without attr name.
        attr_name = self.get_attr_name(target, parent_oid=source)

        # Bad attribute names.
        if attr_name in ('ids', 'refs', '_next'):
            return 80

        # Good attribute names.
        if attr_name in ('_tree', '_blob'):
            return 20

        if (not attr_name) or (attr_name == '_firstbucket'):
            if allowed_look_ahead_depth > 0:
                next_ref_scores = [
                    (
                        bbr,
                        self._get_reference_score(
                            source=bbr,
                            target=source,
                            allowed_look_ahead_depth=allowed_look_ahead_depth - 1
                        )
                    )
                    for bbr
                    in self.get_back_references(source)
                ]
                if next_ref_scores:
                    next_ref_scores.sort(key=lambda x: x[1])
                    (ref, score) = next_ref_scores[0]
                    if score <= 50:
                        return 30

            return 70  # Default score for refs without attr name.

        # Default score.
        return 50

    @instance.memoize
    def _oid_path_to_id_path(self, oid_path):
        result = tuple(
            self.get_id_or_attr_name(child, parent_oid=parent)
            for (child, parent)
            in pairwise(oid_path)
        )
        return result
        self._logger.info('_oid_path_to_id_path: {}'.format(result))

    def _build_reference_maps_from_scratch(self):
        self._reference_map = {}
        self._back_reference_map = {}
        self._oids = set()
        next_oids = {self.root_oid}

        while next_oids:
            current_oid = next_oids.pop()
            if current_oid in self._oids:
                continue

            self._oids.add(current_oid)

            (p, _) = self.storage.load(current_oid)
            refs = set(referencesf(p))
            if not refs:
                continue

            if current_oid in self._reference_map:
                raise RuntimeError(
                    'OID {} already in reference map!'.format(self.oid_to_repr(current_oid))
                )

            next_oids.update(refs)
            self._reference_map[current_oid] = refs
            for r in refs:
                self._back_reference_map.setdefault(r, set()).add(current_oid)

    def _get_reference_cache_path(self):
        cache_dir = os.path.join(os.path.expanduser('~'), '.cache', PACKAGE_NAME)
        last_tid = tid_repr(self.connection.db().lastTransaction())
        return os.path.join(cache_dir, 'zodb_references_{}'.format(last_tid))

    def _load_reference_cache(self, path):
        self._reference_map = {}
        self._back_reference_map = {}
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                (source, target) = (self.repr_to_oid(r) for r in line.split())
                self._reference_map.setdefault(source, set()).add(target)
                self._back_reference_map.setdefault(target, set()).add(source)
        self._oids = set(self._reference_map)
        self._oids.update(self._back_reference_map)

    def _store_reference_cache(self, path):
        mkdirp(os.path.dirname(path))
        with open(path, 'w') as f:
            for (source, targets) in sorted(self.reference_map.iteritems(), key=lambda i: i[0]):
                source_repr = self.oid_to_repr(source)
                for target in sorted(targets):
                    f.write('{} {}\n'.format(source_repr, self.oid_to_repr(target)))


@case
class OIDInfo(object):
    u"""Dumb container of information about an OID.

    This class must be kept simple and static, holding only strings or sequences of strings. The
    main goal of this class is to provide a quick way to show information about an OID.

    WARNING: The `obj` attribute is not a reference to the real object, its actually its string
        representation, i.e `str(obj)`.
    """

    def __init__(self, oid, id=None, obj=None, path=None, oid_path=None, id_path=None):
        pass

    def __str__(self):
        # `as_dict()` returns an OrderedDict.
        parts = ((k, self._field_to_str(k, v)) for (k, v) in self.as_dict().iteritems())
        return '\n'.join('{}: {}'.format(k, v) for (k, v) in parts)

    def __unicode__(self):
        return self.__str__().decode('utf8')

    def _field_to_str(self, name, value):
        if value is None:
            return value

        if name == 'path':
            return '/'.join(value)

        if name == 'oid_path':
            return '/'.join(oid_repr(i) for i in reversed(value))

        if name == 'id_path':
            return '/'.join(str(i) for i in reversed(value))

        return value
