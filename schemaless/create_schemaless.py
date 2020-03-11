# Lint as: python3
"""Convert departmental data files into a schemaless csv.

If you run this with a Planning and PTS file specified, it will
dump those into a schemaless csv. If you provide the files
and also set the --diff flag, it will diff against an existing
schemaless csv.
"""

import argparse
from concurrent import futures
import csv
from csv import DictReader
from datetime import date
from datetime import datetime
import os
import requests
import shutil
import sys
import tempfile
from textwrap import dedent

import schemaless.mapblklot_generator as mapblklot_gen
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import OEWDPermits
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Planning
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
            valid_keys = source.field_names()
            for line in source.yield_records():
                fk = source.foreign_key(line)
                for (key, val) in line.items():
                    if key not in valid_keys:
                        continue
                    writer.writerow([
                        fk,
                        source.NAME,
                        last_updated,
                        key,
                        val.strip().replace('\n', ' ')
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
            valid_keys = source.field_names()
            for line in source.yield_records():
                fk = source.foreign_key(line)
                if fk not in records[source.NAME]:
                    records[source.NAME][fk] = {}
                for (key, val) in line.items():
                    if key not in valid_keys:
                        continue
                    if val != records[source.NAME][fk].get(key, None):
                        records[source.NAME][fk][key] = val
                        writer.writerow([
                            fk,
                            source.NAME,
                            last_updated,
                            key,
                            val.strip().replace('\n', ' ')
                        ])


def get(destdir, src):
    dest = os.path.join(destdir, "%s.csv" % src.NAME)
    print("Fetching %s to %s" % (src.DATA_SF_DOWNLOAD, dest))
    with requests.get(src.DATA_SF_DOWNLOAD, stream=True) as req:
        req.raise_for_status()
        with open(dest, 'wb') as outf:
            for chunk in req.iter_content(chunk_size=8192):
                if chunk:
                    outf.write(chunk)
    print("done with %s" % dest)
    return dest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=dedent(
            """\
            Create the schemaless file from a collection of source data.

            Usage: python -m schemaless.create_schemaless [flags]

            With no flags, all datasources defined in schemeless.sources will
            be downloaded to a tempdir. If any source file is manually
            supplied, that file will be used and the corresponding file will
            not be downloaded.

            To turn off automatic downloads, pass --no_download.
        """))
    parser.add_argument('--no_download', type=bool, default=False,
                        help="Don't download source data.")
    parser.add_argument('--planning_file', help='Planning file', default='')
    parser.add_argument('--pts_file', help='PTS file', default='')
    parser.add_argument('--tco_file', help='TCO file', default='')
    parser.add_argument('--mohcd_pipeline_file',
                        help='MOHCD Pipeline file', default='')
    parser.add_argument('--mohcd_inclusionary_file',
                        help='MOHCD Inclusionary file', default='')
    parser.add_argument('--affordable_file',
                        help='AffordableRentalPortfolio file', default='')
    parser.add_argument('--permit_addenda_file',
                        help='Permit Addenda file', default='')
    parser.add_argument('--oewd_permits_file', help='OEWD permits file',
                        default='')
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
    parser.add_argument('--parcel_data_file')
    args = parser.parse_args()

    the_date = None
    if args.the_date:
        the_date = datetime.strptime(args.the_date, "%Y-%m-%d").date()

    if args.parcel_data_file:
        mapblklot_gen.init(args.parcel_data_file)

    sources = []
    dl_sources = {}
    destdir = tempfile.mkdtemp()

    with futures.ThreadPoolExecutor(
            thread_name_prefix="schemaless-download") as executor:
        for (source, arg) in [
                (Planning, args.planning_file),
                (PTS, args.pts_file),
                (TCO, args.tco_file),
                (MOHCDPipeline, args.mohcd_pipeline_file),
                (MOHCDInclusionary, args.mohcd_inclusionary_file),
                (PermitAddendaSummary, args.permit_addenda_file),
                (AffordableRentalPortfolio, args.affordable_file),
                (OEWDPermits, args.oewd_permits_file)]:
            if arg:
                sources.append(source(arg))
            elif source.DATA_SF_DOWNLOAD and not args.no_download:
                dl_sources[executor.submit(get, destdir, source)] = source
            else:
                print("Skipping %s" % source.NAME)

        for future in futures.as_completed(dl_sources):
            try:
                src = dl_sources[future]
                sources.append(src(future.result()))
            except Exception as e:
                print("Error downloading data for %s: %s" % (
                      src.NAME, e))
                raise

    if len(sources) == 0:
        parser.print_help()
        print('\nERROR: at least one source must be specified.')
        sys.exit(1)

    if not args.diff:
        just_dump(sources, args.out_file, the_date)
    else:
        dump_and_diff(sources, args.out_file, args.diff, the_date)
