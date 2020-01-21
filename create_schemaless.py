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
import lzma
import shutil
import sys
import uuid

blocked_fields = ['acalink', 'aalink']
csv.field_size_limit(sys.maxsize)

def just_dump(ppts_file, outfile):
    if ppts_file.endswith('.xz'):
        o = lzma.open
    else:
        o = open
    with o(ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
        reader = DictReader(inf)
        today = date.today()
        with open(outfile, 'w') as outf:
            writer = csv.writer(outf)
            writer.writerow(['id', 'fk', 'source', 'last_updated', 'name', 'value'])
            source = 'ppts'
            last_updated = today.isoformat()
            for line in reader:
                id = uuid.uuid4()
                fk = line['record_id']
                cat = line['record_type_category']
                # skip non-PRJs
                if cat != "PRJ":
                    continue

                for (key,val) in line.items():
                    if val and key not in blocked_fields:
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
    record_id_to_uuid = {}
    for uid, record in records.items():
        rid = record['record_id']
        if not rid:
            continue
        if rid in record_id_to_uuid and record_id_to_uuid[rid] != uid:
            raise RuntimeError(
                "record_id %s points to multiple UUIDS: %s and %s" %
                (riw, uid, record_id_to_uuid[rid]))
        record_id_to_uuid[rid] = uid
    print("%s records to uuids" % len(record_id_to_uuid))

    if ppts_file.endswith('.xz'):
        o = lzma.open
    else:
        o = open
    with o(ppts_file, mode='rt', encoding='utf-8', errors='replace') as inf:
        reader = DictReader(inf)
        today = date.today()
        shutil.copyfile(schemaless_file, outfile)
        with open(outfile, 'a') as outf:
            writer = csv.writer(outf)
            source = 'ppts'
            last_updated = today.isoformat()
            for line in reader:
                rid = line['record_id']
                if rid not in record_id_to_uuid:
                    id = uuid.uuid4()
                    record_id_to_uuid[rid] = id
                id = record_id_to_uuid[rid]
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
