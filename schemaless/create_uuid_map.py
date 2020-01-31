import argparse
from collections import OrderedDict
import csv
from csv import DictReader
import uuid

from fileutils import open_file
from schemaless.create_schemaless import latest_values
from schemaless.sources import source_map
from schemaless.sources import PPTS
from schemaless.sources import PTS

class RecordGraph:
    def __init__(self):
        self._nodes = OrderedDict()

    @classmethod
    def from_files(cls, schemaless_file, uuid_map_file):
        """Build a RecordGraph from schemaless & uuid_map files."""
        rg = cls()

        latest_records = latest_values(schemaless_file)

        # Create a mapping between permit numbers and their associated PPTS
        # record (so we an ensure they are assigned the same UUID).
        permit_number_to_ppts = {}
        for fk, record in latest_records[PPTS.NAME].items():
            if record['building_permit_id']:
                for permit_number in record['building_permit_id'].split(","):
                    #  This assumes that a given permit can only have one PPTS
                    #  parent. Is that accurate? 
                    permit_number_to_ppts[permit_number] = fk

        # Read the latest values from the schemaless file to build the graph.
        for source, source_records in latest_records.items():
            for fk, record in source_records.items():
                parents = []
                children = []
                if source == PPTS.NAME:
                    if record['parents']:
                        parents = record['parents'].split(",")
                    if record['children']:
                        children = record['children'].split(",")

                if source == PTS.NAME:
                    if permit_number_to_ppts[record['permit_number']]:
                      parents = [permit_number_to_ppts[record['permit_number']]]

                the_date = None

                if source_map[source].DATE_KEY in record:
                    the_date = source_map[source].get_date(record)
                rg.add(Node(
                    record_id=fk,
                    date=the_date,
                    parents=parents,
                    children=children,
                    uuid=None,
                ))

        # Read existing record_id->uuid mapping from the existing schemaless
        # map and update nodes with exisitng UUIDs.
        if uuid_map_file != '':
            with open_file(uuid_map_file, 'r') as f:
                reader = DictReader(f)
                for line in reader:
                    fk = line['fk']
                    if fk in rg:
                        rg._nodes[fk].uuid = line['uuid']
                    else:
                        print("Error: unknown id %s" % fk)

        # Resolve all parent UUIDs and generate new UUIDs
        rg._assign_uuids()
        return rg

    def to_file(self, outfile):
        """Dump the uuid->fk map."""
        with open_file(outfile, 'w') as f:
            # We use an OrderedDict for self._nodes, so ensure a consistent
            # ordering across runs.
            writer = csv.writer(f)
            writer.writerow(['uuid', 'fk'])
            for fk, record in self._nodes.items():
                writer.writerow([record.uuid, record.record_id])

    def add(self, record):
        rid = record.record_id
        if rid in self._nodes:
            # This may either be an update or adding a parent/child for an
            # existing node.
            node = self._nodes[rid]
            node.date = record.date
            if not node.uuid and record.uuid:
                node.uuid = record.uuid
            node.parents.update(record.parents)
            node.children.update(record.children)
            record = node
        else:
            self._nodes[rid] = record

        for parent in record.parents:
            self.link(parent, record.record_id)
        for child in record.children:
            self.link(record.record_id, child)

    def link(self, parent_record_id, child_record_id):
        if parent_record_id not in self._nodes:
            self._nodes[parent_record_id] = Node(parent_record_id)
        if child_record_id not in self._nodes:
            self._nodes[child_record_id] = Node(child_record_id)
        self._nodes[parent_record_id].add_child(child_record_id)
        self._nodes[child_record_id].add_parent(parent_record_id)

    def get(self, record_id):
        return self._nodes.get(record_id, None)

    def _resolve_parent(self, record_id):
        record = self.get(record_id)
        # Note: by not short-circuiting when a record already has a UUID, we
        # ensure that children who are reassigned to new parents always get the
        # correct parent id.
        if not record.parents:
            return record
        all_parents = []
        for idx, pid in enumerate(record.parents):
            if pid not in self._nodes or self._nodes[pid].date is None:
                # This implies this record is bad data and cannot be properly
                # connected to a real parent record.
                continue
            all_parents.append(self._resolve_parent(pid))
        # If all_parents is empty, then none of the parents in
        # record['parents'] actually exist as valid records. So we can't link
        # this to an exisitng uuid, so just return itself.
        # # TODO: Come back to this when we are reading in DBI. At least a few
        # of these records have related building permits.
        if not all_parents:
            return record
        # Sort on the tuple of (date, record_id) so we have a stable
        # ordering for the same dates.
        return sorted(all_parents,
                      key=lambda x: (x.date, x.record_id),
                      reverse=True)[0]

    def _assign_uuids(self):
        seen = set()
        for fk, record in self._nodes.items():
            if fk in seen:
                continue
            # 'parent' is a bit of a misnomer -- it may be itself!
            parent = self._resolve_parent(fk)
            puid = parent.uuid
            if not puid:
                if record.uuid:
                    puid = record.uuid
                else:
                    puid = uuid.uuid4()
                self.get(parent.record_id).uuid = puid
            self.get(fk).uuid = puid
            seen.add(fk)

    def __contains__(self, obj):
        return obj in self._nodes

    def __len__(self):
        return len(self._nodes)

    def items(self):
        return self._nodes.items()

    def keys(self):
        return self._nodes.keys()

    def values(self):
        return self._nodes.values()


class Node:
    def __init__(self,
                 record_id,
                 date=None,
                 parents=None,
                 children=None,
                 uuid=None):
        self.record_id = record_id
        self.date = date
        if parents is None:
            self.parents = set()
        else:
            self.parents = set(parents)
        if children is None:
            self.children = set()
        else:
            self.children = set(children)
        self.uuid = uuid

    def add_child(self, child_record_id):
        self.children.add(child_record_id)

    def add_parent(self, parent_record_id):
        self.parents.add(parent_record_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('schemaless_file', help='Schemaless csv file')
    parser.add_argument(
            '--uuid_map_file',
            help='UUID mapping file',
            default='')
    parser.add_argument('outfile', help='Output path of uuid mapping')
    args = parser.parse_args()

    rg = RecordGraph.from_files(args.schemaless_file, args.uuid_map_file)
    rg.to_file(args.outfile)
