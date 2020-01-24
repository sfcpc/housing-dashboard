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


def _open(fname, *args, **kwargs):
    if fname.endswith('.xz'):
        o = lzma.open
    else:
        o = open
    return o(fname, *args, **kwargs)


def resolve_parent(record_id_metadata, record_id):
    record = record_id_metadata[record_id]
    if record['uuid']:
        # Either implies this is a root or we have already resolved the parent
        return record
    if not record['parents']:
        return record
    all_parents = []
    for idx, pid in enumerate(record['parents']):
        if pid not in record_id_metadata:
            # This implies this record is bad data and cannot be properly
            # connected to a real parent record. 
            continue
        all_parents.append(resolve_parent(record_id_metadata, pid))
    # If all_parents is empty, then none of the parents in record['parents']
    # actually exist as valid records. So we can't link this to an exisitng
    # uuid, so just return itself.
    # # TODO: Come back to this when we are reading in DBI. At least a few
    # of these records have related building permits.
    if not all_parents:
        return record
    return sorted(all_parents,
                  key=lambda x: x['date_opened'],
                  reverse=True)[0]


def resolve_all_parents(record_id_metadata):
    for fk, val in record_id_metadata.items():
        if val['uuid']:
            continue
        puid = resolve_parent(record_id_metadata, fk)['uuid']
        if not puid:
            puid = uuid.uuid4()
        record_id_metadata[fk]['uuid'] = puid
    return record_id_metadata


def _map_children_to_parents(ppts_file, record_id_metadata = None):
    if record_id_metadata is None:
        record_id_metadata = {}
    # This looks dumb, but the easiest way to ensure the parent->child mapping
    # exists is to read through the file twice.
    with _open(ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
        reader = DictReader(inf)
        for line in reader:
            fk = line['record_id']
            if fk in record_id_metadata:
                continue

            parents = []
            if line['parent']:
                parents = line['parent'].split(',')
            record_id_metadata[fk] = {
                'uuid': None,
                'date_opened': datetime.strptime(line['date_opened'].split(" ")[0], '%m/%d/%Y'),
                'parents': parents,
            }
            if not parents:
                record_id_metadata[fk]['uuid'] = uuid.uuid4()

    record_id_metadata = resolve_all_parents(record_id_metadata)
    # At this point, all records have been associated with their parents and
    # have a unique ID linking them together.
    return record_id_metadata


def just_dump(ppts_file, outfile):
    record_id_metadata = _map_children_to_parents(ppts_file)
    with _open(ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
        reader = DictReader(inf)
        today = date.today()
        with open(outfile, 'w') as outf:
            writer = csv.writer(outf)
            writer.writerow(['id', 'fk', 'source', 'last_updated', 'name', 'value'])
            source = 'ppts'
            last_updated = today.isoformat()
            for line in reader:
                fk = line['record_id']
                id = record_id_metadata[fk]['uuid']
                for (key,val) in line.items():
                    if val:
                        writer.writerow([id, fk, source, last_updated, key, val])



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
    record_id_metadata = {}
    # Read existing record_id->uuid mapping from the existing schemaless file
    for uid, record in records.items():
        rid = record['record_id']
        if rid in record_id_metadata and record_id_metadata[rid]['uuid'] != uid:
            raise RuntimeError(
                "record_id %s points to multiple UUIDS: %s and %s" %
                (riw, uid, record_id_metadata[rid]['uuid']))
        parents = []
        if record['parents']:
            parents = record['parents'].split(",")
        record_id_metadata[rid] = {
            'uuid': uid,
            'date_opened': datetime.strptime(record['date_opened'].split(" ")[0], '%m/%d/%Y'),
            'parents': parents,
        }

    print("%s records to uuids" % len(record_id_metadata))

    # Add new child-parent relationships
    record_id_metadata = _map_children_to_parents(ppts_file, record_id_metadata)

    with _open(ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
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
                id = record_id_metadata[rid]['uuid']
                for (key,val) in line.items():
                    if val and val != records[id][key]:
                        writer.writerow([id, rid, source, last_updated, key, val])


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
