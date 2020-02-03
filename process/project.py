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
from schemaless.sources import PTS


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

        for (key, nvs) in self._data.items():
            self._data[nv.key].sort(key=lambda nv: nv.last_updated)

    def latest_name_values(self):
        """Gets a dict snapshot of the latest name values for this entry."""
        result = {}
        for (key, nvs) in self._data.items():
            result[key] = nvs[-1].value
        return result

    def num_name_values(self):
        return len(self._data)

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

    def __init__(self, id, entries, recordgraph):
        """Initializes the Project.

        id: the unique id we use to identify all related db entries as related
            to a given project.
        entries: a list of Entry that corresponds to all db entries for a
            project, as determined by our uuid mapping of fks to a uuid
        recordgraph:  a fully built record graph that contains any parent-child
            relationships for the entries.  This class will not mutate it.
        """
        self.id = id
        self.recordgraph = recordgraph

        # find root entries so we know where to start looking
        self.roots = defaultdict(list)
        self.children = defaultdict(list)
        for entry in entries:
            node = self.recordgraph.get(entry.fk)
            if not node:
                print('Warning: found an entry not in our record graph: %s'
                      % entry.fk)
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

    def field(self, name):
        """Fetches the value for a field, using some business logic.

        The process of getting a field:
        1. Check pts. Start with root project and descend to children. If
           none found:
        2. Check ppts. Start with root project and descend to children only
           if none found on the root.

        Returns:
            string
        """
        # TODO: I'm not even sure this is the correct logic to use for dealing
        # with ambiguities.
        result = (None, datetime.min)
        for source in [PTS.NAME, PPTS.NAME]:
            parents = self.roots[source]

            if len(parents) > 0:
                latest = (None, datetime.min)
                for parent in parents:
                    val = parent.get_latest(name)
                    if val and val[1] > latest[1]:
                        latest = val
                if not latest[0]:
                    children = self.children[source]
                    if len(children) > 0:
                        for child in children:
                            val = child.get_latest(name)
                            if val and val[1] > latest[1]:
                                latest = val

                if latest[0] and latest[1] > result[1]:
                    result = latest

                if result[0]:
                    break

        return result[0] if result[0] else ''
