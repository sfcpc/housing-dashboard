# Lint as: python3
"""Convert a PPTS file into a schemaless csv.

If you run this with a single arg, it will just dump the PPTS file to a
schemaless csv. If you provide two args, it will diff against an existing
schemaless csv.
"""

import argparse
from collections import defaultdict
import csv
from csv import DictReader
from datetime import date
from datetime import datetime
import lzma
import shutil
import sys
import uuid

csv.field_size_limit(sys.maxsize)


fields = {
    'ppts': {
        'record_id': 'record_id',
        'record_type': 'record_type',
        'record_type_category': 'record_type_category',
        'record_name': 'name',
        'description': 'description',
        'parent': 'parent',
        'children': 'children',
        'record_status': 'status',
        'date_opened': 'date_opened',
        'date_closed': 'date_closed',
        # Location details
        'address': 'address',
        'the_geom': 'the_geom',

        # Developer and Planner
        'developer_name': 'TODO',
        'planner_name': 'planner_name',
        'planner_email': 'planner_email',
        'planner_phone': 'planner_phone',

        # Child record details
        'incentives': 'TODO',
        'ppa_submitted': 'TODO',
        'ppa_letter_issued': 'TODO',
        'prj_submitted': 'TODO',
        'nia_issued': 'TODO',
        'application_accepted': 'TODO',
        'pcl_issued': 'TODO',
        'project_desc_stable': 'TODO',
        'env_review_type': 'TODO',
        'first_hearing': 'TODO',
        'final_hearing': 'TODO',
        'entitlements_issued': 'TODO',

        # Unit/land use details
        'non_housing_uses': 'TODO',
        'RELATED_BUILDING_PERMIT': 'building_permit_id',
        'LAND_USE_RESIDENTIAL_EXIST': 'residential_sq_ft_existing',
        'LAND_USE_RESIDENTIAL_PROP': 'residential_sq_ft_proposed',
        'LAND_USE_RESIDENTIAL_NET': 'residential_sq_ft_net',
        'ADU': 'is_adu',  # TOOD: Normalize this bool? True == "CHECKED"
        'PRJ_FEATURE_AFFORDABLE_EXIST': 'affordable_units_existing',
        'PRJ_FEATURE_AFFORDABLE_PROP': 'affordable_units_proposed',
        'PRJ_FEATURE_AFFORDABLE_NET': 'affordable_units_net',
        'PRJ_FEATURE_MARKET_RATE_EXIST': 'market_rate_units_existing',
        'PRJ_FEATURE_MARKET_RATE_PROP': 'market_rate_units_proposed',
        'PRJ_FEATURE_MARKET_RATE_NET': 'market_rate_units_net',
        'PRJ_FEATURE_PARKING_EXIST': 'parking_sq_ft_exist',
        'PRJ_FEATURE_PARKING_PROP': 'parking_sq_ft_proposed',
        'PRJ_FEATURE_PARKING_NET': 'parking_sq_ft_net',
        'RESIDENTIAL_STUDIO_EXIST': 'residential_units_studio_existing',
        'RESIDENTIAL_STUDIO_PROP': 'residential_units_studio_proposed',
        'RESIDENTIAL_STUDIO_NET': 'residential_units_studio_net',
        'RESIDENTIAL_1BR_EXIST': 'residential_units_1br_existing',
        'RESIDENTIAL_1BR_PROP': 'residential_units_1br_proposed',
        'RESIDENTIAL_1BR_NET': 'residential_units_1br_net',
        'RESIDENTIAL_2BR_EXIST': 'residential_units_2br_existing',
        'RESIDENTIAL_2BR_PROP': 'residential_units_2br_proposed',
        'RESIDENTIAL_2BR_NET': 'residential_units_2br_net',
        'RESIDENTIAL_3BR_EXIST': 'residential_units_3br_existing',
        'RESIDENTIAL_3BR_PROP': 'residential_units_3br_proposed',
        'RESIDENTIAL_3BR_NET': 'residential_units_3br_net',
        'RESIDENTIAL_ADU_STUDIO_EXIST': 'residential_units_adu_studio_existing',  # NOQA
        'RESIDENTIAL_ADU_STUDIO_PROP': 'residential_units_adu_studio_proposed',
        'RESIDENTIAL_ADU_STUDIO_NET': 'residential_units_adu_studio_net',
        'RESIDENTIAL_ADU_STUDIO_AREA': 'residential_sq_ft_adu_studio',
        'RESIDENTIAL_ADU_1BR_EXIST': 'residential_units_adu_1br_existing',
        'RESIDENTIAL_ADU_1BR_PROP': 'residential_units_adu_1br_proposed',
        'RESIDENTIAL_ADU_1BR_NET': 'residential_units_adu_1br_net',
        'RESIDENTIAL_ADU_1BR_AREA': 'residential_sq_ft_adu_1br',
        'RESIDENTIAL_ADU_2BR_EXIST': 'residential_units_adu_2br_existing',
        'RESIDENTIAL_ADU_2BR_PROP': 'residential_units_adu_2br_proposed',
        'RESIDENTIAL_ADU_2BR_NET': 'residential_units_adu_2br_net',
        'RESIDENTIAL_ADU_2BR_AREA': 'residential_sq_ft_adu_2br',
        'RESIDENTIAL_ADU_3BR_EXIST': 'residential_units_adu_3br_existing',
        'RESIDENTIAL_ADU_3BR_PROP': 'residential_units_adu_3br_proposed',
        'RESIDENTIAL_ADU_3BR_NET': 'residential_units_adu_3br_net',
        'RESIDENTIAL_ADU_3BR_AREA': 'residential_sq_ft_adu_3br',
        'RESIDENTIAL_SRO_EXIST': 'residential_units_sro_existing',
        'RESIDENTIAL_SRO_PROP': 'residential_units_sro_proposed',
        'RESIDENTIAL_SRO_NET': 'residential_units_sro_net',
        'RESIDENTIAL_MICRO_EXIST': 'residential_units_micro_existing',
        'RESIDENTIAL_MICRO_PROP': 'residential_units_micro_proposed',
        'RESIDENTIAL_MICRO_NET': 'residential_units_micro_net',
    }
}


def _open(fname, *args, **kwargs):
    if fname.endswith('.xz'):
        o = lzma.open
    else:
        o = open
    return o(fname, *args, **kwargs)


class RecordGraph:
    def __init__(self):
        self._nodes = {}

    def add(self, record):
        rid = record.record_id
        if rid in self._nodes:
            # This may either be an update or adding a parent/child for an
            # existing node.
            node = self._nodes[rid]
            node.date_opened = record.date_opened
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
            self._nodes[parent_record_id] = RecordMetadata(parent_record_id)
        if child_record_id not in self._nodes:
            self._nodes[child_record_id] = RecordMetadata(child_record_id)
        self._nodes[parent_record_id].add_child(child_record_id)
        self._nodes[child_record_id].add_parent(parent_record_id)

    def get(self, record_id):
        return self._nodes.get(record_id, None)

    def resolve_parent(self, record_id):
        record = self.get(record_id)
        if record.uuid:
            # Either implies this is a root or we have already resolved the parent
            return record
        if not record.parents:
            return record
        all_parents = []
        for idx, pid in enumerate(record.parents):
            if pid not in self._nodes or self._nodes[pid].date_opened is None:
                # This implies this record is bad data and cannot be properly
                # connected to a real parent record.
                continue
            all_parents.append(self.resolve_parent(pid))
        # If all_parents is empty, then none of the parents in record['parents']
        # actually exist as valid records. So we can't link this to an exisitng
        # uuid, so just return itself.
        # # TODO: Come back to this when we are reading in DBI. At least a few
        # of these records have related building permits.
        if not all_parents:
            return record
        return sorted(all_parents,
                      key=lambda x: x.date_opened,
                      reverse=True)[0]

    def resolve_all_parents(self):
        for fk, record in self._nodes.items():
            if record.uuid:
                continue
            # 'parent' is a bit of a misnomer -- it may be itself!
            parent = self.resolve_parent(fk)
            puid = parent.uuid
            if not puid:
                puid = uuid.uuid4()
                self.get(parent.record_id).uuid = puid
            self.get(fk).uuid = puid

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


class RecordMetadata:
    def __init__(self,
                 record_id,
                 date_opened=None,
                 parents=None,
                 children=None,
                 uuid=None):
        self.record_id = record_id
        self.date_opened = date_opened
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


def _map_children_to_parents(ppts_file, record_graph=None):
    if record_graph is None:
        record_graph = RecordGraph()
    # This looks dumb, but the easiest way to ensure the parent->child mapping
    # exists is to read through the file twice.
    with _open(
            ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
        reader = DictReader(inf)
        for line in reader:
            fk = line['record_id']
            if fk in record_graph:
                continue

            parents = []
            children = []
            if line['parent']:
                parents = line['parent'].split(',')
            if line['children']:
                children = line['children'].split(",")
            record_graph.add(RecordMetadata(
                record_id=fk,
                date_opened=datetime.strptime(
                    line['date_opened'].split(" ")[0], '%m/%d/%Y'),
                parents=parents,
                children=children,
            ))

    record_graph.resolve_all_parents()
    # At this point, all records have been associated with their parents and
    # have a unique ID linking them together.
    return record_graph


def just_dump(ppts_file, outfile):
    record_graph = _map_children_to_parents(ppts_file)
    with _open(
            ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
        reader = DictReader(inf)
        today = date.today()
        with open(outfile, 'w') as outf:
            writer = csv.writer(outf)
            writer.writerow(
                ['id', 'fk', 'source', 'last_updated', 'name', 'value'])
            source = 'ppts'
            last_updated = today.isoformat()
            for line in reader:
                fk = line['record_id']
                id = record_graph.get(fk).uuid
                for (key, val) in line.items():
                    if key not in fields[source]:
                        continue
                    if val:
                        writer.writerow(
                            [id, fk, source, last_updated,
                             fields[source][key], val])


def latest_values(schemaless_file):
    """Collapse the schemaless file into the latest values for each record."""
    records = defaultdict(lambda: defaultdict(str))
    with open(schemaless_file, 'r') as inf:
        reader = DictReader(inf)
        for line in reader:
            records[line['id']][line['name']] = line['value']
    return records


def dump_and_diff(ppts_file, outfile, schemaless_file):
    records = latest_values(schemaless_file)
    print("Loaded %d records" % len(records))
    print("%s unique records" % len(records))
    record_graph = RecordGraph()
    # Read existing record_id->uuid mapping from the existing schemaless file
    for uid, record in records.items():
        rid = record['record_id']
        if (rid in record_graph
                and record_graph.get(rid).uuid != uid):
            raise RuntimeError(
                "record_id %s points to multiple UUIDS: %s and %s" %
                (rid, uid, record_graph.get(rid).uuid))
        parents = []
        children = []
        if record['parents']:
            parents = record['parents'].split(",")
        if record['children']:
            children = record['children'].split(",")
        record_graph.add(RecordMetadata(
            record_id=rid,
            uuid=uid,
            date_opened=datetime.strptime(
                record['date_opened'].split(" ")[0], '%m/%d/%Y'),
            parents=parents,
            children=children,
        ))

    print("%s records to uuids" % len(record_graph))

    # Add new child-parent relationships
    record_graph = _map_children_to_parents(ppts_file, record_graph)

    with _open(
            ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
        reader = DictReader(inf)
        today = date.today()
        shutil.copyfile(schemaless_file, outfile)
        with open(outfile, 'a') as outf:
            writer = csv.writer(outf)
            source = 'ppts'
            last_updated = today.isoformat()
            for line in reader:
                rid = line['record_id']
                # If this record has a parent, use the parent's UUID
                id = record_graph.get(rid).uuid
                for (key, val) in line.items():
                    if key not in fields[source]:
                        continue
                    if val and val != records[id][key]:
                        writer.writerow(
                            [id, rid, source, last_updated,
                             fields[source][key], val])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('ppts_file', help='PPTS file')
    parser.add_argument('out_file', help='output file for schemaless csv')
    parser.add_argument(
        '--diff',
        help='A schemaless csv generated by this script, to diff against.',
        default='')
    args = parser.parse_args()

    if not args.diff:
        just_dump(args.ppts_file, args.out_file)
    else:
        dump_and_diff(args.ppts_file, args.out_file, args.diff)
