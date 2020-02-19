# Lint as: python3
"""Contains logic to create a unified view of a 'project.'

Project information can come from multiple data sources with multiple
records, and this contains all the handling to make sense of it all.
"""
import bisect

from collections import defaultdict
from collections import namedtuple
from datetime import datetime

from schemaless.sources import PPTS


NameValue = namedtuple('NameValue',
                       ['key', 'value', 'last_updated'],
                       defaults=[None, None, datetime.min])


class Entry:
    """An 'entry' is a record for a project in some database somewhere."""

    def __init__(self, fk, source, namevalues):
        """
        fk = foreign key;
        source = e.g. planning;
        namevalues = a list of NameValue
        """
        self.fk = fk
        self.source = source
        self._data = defaultdict(list)
        for nv in namevalues:
            self._data[nv.key].append(nv)

        for k in self._data.keys():
            self._data[k].sort(key=lambda nv: nv.last_updated)

    def latest_name_values(self):
        """Gets a dict snapshot of the latest name values for this entry."""
        result = {}
        for (key, nvs) in self._data.items():
            result[key] = nvs[-1].value
        return result

    def num_name_values(self):
        c = 0
        for (key, nvs) in self._data.items():
            c += len(nvs)
        return c

    def add_name_value(self, new_nv):
        """Makes sure sort order is maintained for new name values"""
        if len(self._data[new_nv.key]) == 0:
            self._data[new_nv.key].append(new_nv)
            return

        dates = [nv.last_updated for nv in self._data[new_nv.key]]
        self._data[new_nv.key].insert(bisect.bisect_left(dates,
                                                         new_nv.last_updated),
                                      new_nv)

    def get_latest(self, key):
        """
        Returns:
          A tuple of (string, datetime) representing the value for key and
          when it was updated in the schema-less file.  If no value found for
          the key, None is returned.
        """
        nvs = self._data.get(key, [])
        return (nvs[-1].value, nvs[-1].last_updated) if len(nvs) > 0 else None

    def oldest_name_value(self):
        """
        Returns:
          The oldest update time we have for any name value on this entry.
        """
        oldest = datetime.max
        for (key, nvs) in self._data.items():
            if nvs[0].last_updated < oldest:
                oldest = nvs[0].last_updated
        return oldest


class Project:
    """A way to abstract some of the details of handling multiple records for a
    project, from multiple sources."""

    def __str__(self):
        return '%s => { roots: %s, children %s }' % (
            self.id, self.roots, self.children)

    def __init__(self, id, entries, recordgraph):
        """Initializes the Project.

        id: the unique id we use to identify all related db entries as related
            to a given project.
        entries: a list of Entry that corresponds to all db entries for a
            project, as determined by our uuid mapping of fks to a uuid
        recordgraph:  a fully built record graph that contains any parent-child
            relationships for the entries.  This class will not mutate it.

        Raises ValueError if the provided entries do not craete a valid
        project.
        """
        self.id = id
        self.recordgraph = recordgraph

        # find root entries so we know where to start looking
        self.roots = defaultdict(list)
        self.children = defaultdict(list)
        for entry in entries:
            node = self.recordgraph.get(entry.fk)
            if not node:
                print('Warning: entry not in our record graph: %s from %s'
                      % (entry.fk, entry.source))
                continue

            if len(node.parents) == 0:
                self.roots[entry.source].append(entry)
            else:
                self.children[entry.source].append(entry)

        if len(self.roots) == 0:
            # upgrade oldest child
            oldest_child = None
            for (source, entries) in self.children.items():
                for entry in entries:
                    if (not oldest_child or
                            entry.oldest_name_value() <
                            oldest_child.oldest_name_value()):
                        oldest_child = entry

            if oldest_child:
                self.roots[oldest_child.source].append(oldest_child)
                self.children[oldest_child.source].remove(oldest_child)

        # REMOVE THIS IF WE ARE EVER DETERMINED TO GET DATA FROM PRJ-LESS
        # RECORDS
        if (len(self.roots) == 0 or
                len(self.roots.get(PPTS.NAME, [])) == 0):
            msg = 'No root PRJ record for project'
            if len(self.roots) > 0:
                # get an arbitrary root to extract a FK, so just get
                # the first entry for the first root we find
                msg = msg + (' "%s"' % list(self.roots.items())[0][1][0].fk)
            raise ValueError(msg)

    def _test_entry_predicate(self, entry, entry_predicate):
        if entry_predicate:
            for pred in entry_predicate:
                e = entry.get_latest(pred[0])
                valid_entry = ((e is not None and pred[1](e[0])) or
                               (e is None and pred[1]('')))
                if not valid_entry:
                    return False
        return True

    def fields(self, name, source, entry_predicate=None):
        """Similar to field() but fetches all values for a given field name
        instead of just trying to pick one.

        Unlike field(), returns Entries, where each Entry is one that has a
        value for the given name.

        Returns:
            a dict mapping a foreign key to all related Entries.
        """
        result = {}
        for parent in self.roots.get(source, []):
            if (self._test_entry_predicate(parent, entry_predicate) and
                    parent.get_latest(name)):
                if parent.fk not in result:
                    result[parent.fk] = []
                result[parent.fk].append(parent)

        for child in self.children.get(source, []):
            if (self._test_entry_predicate(child, entry_predicate) and
                    child.get_latest(name)):
                if child.fk not in result:
                    result[child.fk] = []
                result[child.fk].append(child)

        return result

    def field(self, name, source, entry_predicate=None):
        """Fetches the value for a field, using some business logic.

        source is a source.NAME from schemaless.sources

        If specified, entry_predicate is a list of two-element tuples, where
        the first element is a name, and the second element is a lambda that
        can take a string and returns a bool.  entry_predicate is used to make
        sure whatever field value is extracted comes from an entry that at
        least has this other field as well.

        The process of getting a field:
          * Start with root project.
            * If multiple roots, choose one with latest update time.
          * If source != PPTS, then descend to children.
            * If multiple children, choose one with latest update time.
          * If source == PPTS, then only descend to children if None found on
            root.

        Returns:
            string (an empty string if no value found)
        """
        result = (None, datetime.min)

        parents = self.roots[source]

        if len(parents) > 0:
            for parent in parents:
                val = parent.get_latest(name)
                if (val and
                        val[1] > result[1] and
                        self._test_entry_predicate(parent, entry_predicate)):
                    result = val

        if source != PPTS.NAME or result[0] is None:
            for child in self.children[source]:
                val = child.get_latest(name)
                if (val and
                        val[1] > result[1] and
                        self._test_entry_predicate(child, entry_predicate)):
                    result = val

        return result[0] if result[0] else ''
