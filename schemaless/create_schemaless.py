# Lint as: python3
"""Convert departmental data files into a schemaless csv.

If you run this with a PPTS and PTS file specified, it will
dump those into a schemaless csv. If you provide the files
and also set the --diff flag, it will diff against an existing
schemaless csv.
"""

import argparse
import csv
from csv import DictReader
from datetime import date
from datetime import datetime
import shutil
import sys

from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import PPTS
from schemaless.sources import PTS
from schemaless.sources import TCO

csv.field_size_limit(sys.maxsize)


def just_dump(sources, outfile, the_date=None):
    with open(outfile, 'w', newline='\n', encoding='utf-8') as outf:
        writer = csv.writer(outf)
        writer.writerow(['fk', 'source', 'last_updated', 'name', 'value'])
        last_updated = date.today().isoformat()
        if the_date:
            last_updated = the_date.isoformat()

        for source in sources:
            for line in source.yield_records():
                fk = source.foreign_key(line)
                for (key, val) in line.items():
                    writer.writerow([
                            fk, source.NAME, last_updated, key, val.strip()
                    ])


def latest_values(schemaless_file):
    """Collapse the schemaless file into the latest values for each record."""
    records = {}
    with open(schemaless_file, 'r') as inf:
        reader = DictReader(inf)
        for line in reader:
            source, fk, key, val = (
                line['source'], line['fk'], line['name'], line['value'])
            if source not in records:
                records[source] = {}
            if fk not in records[source]:
                records[source][fk] = {}
            records[source][fk][key] = val
    return records


def dump_and_diff(sources, outfile, schemaless_file, the_date=None):
    records = latest_values(schemaless_file)
    print("Loaded %d records" % len(records))

    shutil.copyfile(schemaless_file, outfile)
    with open(outfile, 'a', newline='\n', encoding='utf-8') as outf:
        writer = csv.writer(outf)
        last_updated = date.today().isoformat()
        if the_date:
            last_updated = the_date.isoformat()

        for source in sources:
            for line in source.yield_records():
                fk = source.foreign_key(line)
                if fk not in records[source.NAME]:
                    records[source.NAME][fk] = {}
                for (key, val) in line.items():
                    if val != records[source.NAME][fk].get(key, None):
                        records[source.NAME][fk][key] = val
                        writer.writerow([
                            fk, source.NAME, last_updated, key, val.strip()
                        ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--ppts_file', help='PPTS file', default='')
    parser.add_argument('--pts_file', help='PTS file', default='')
    parser.add_argument('--tco_file', help='TCO file', default='')
    parser.add_argument('--mohcd_pipeline_file',
                        help='MOHCD Pipeline file', default='')
    parser.add_argument('--mohcd_inclusionary_file',
                        help='MOHCD Inclusionary file', default='')
    parser.add_argument('--permit_addenda_file',
                        help='Permit Addenda file', default='')
    parser.add_argument('out_file', help='output file for schemaless csv')

    parser.add_argument(
        '--diff',
        help='A schemaless csv generated by this script, to diff against.',
        default='')
    parser.add_argument(
        '--the-date',
        help=('Date this script was run, optional, defaults to today. '
              'YYYY-MM-DD'),
        default='')
    args = parser.parse_args()

    the_date = None
    if args.the_date:
        the_date = datetime.strptime(args.the_date, "%Y-%m-%d").date()
    sources = []
    if args.ppts_file:
        sources.append(PPTS(args.ppts_file))
    if args.pts_file:
        sources.append(PTS(args.pts_file))
    if args.tco_file:
        sources.append(TCO(args.tco_file))
    if args.mohcd_pipeline_file:
        sources.append(MOHCDPipeline(args.mohcd_pipeline_file))
    if args.mohcd_inclusionary_file:
        sources.append(MOHCDInclusionary(args.mohcd_inclusionary_file))
    if args.permit_addenda_file:
        sources.append(PermitAddendaSummary(args.permit_addenda_file))

    if len(sources) == 0:
        parser.print_help()
        print('\nERROR: at least one source must be specified.')
        sys.exit(1)

    if not args.diff:
        just_dump(sources, args.out_file, the_date)
    else:
        dump_and_diff(sources, args.out_file, args.diff, the_date)
