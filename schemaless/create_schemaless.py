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
import logging
import os
import shutil
import sys
import tempfile
from textwrap import dedent

from datasf import download
from datasf import get_client
import schemaless.mapblklot_generator as mapblklot_gen
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import OEWDPermits
from schemaless.sources import PARCELS_DATA_SF_VIEW_ID
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Planning
from schemaless.sources import PTS
from schemaless.sources import TCO
from schemaless.upload import SCHEMALESS_VIEW_ID
from schemaless.upload import upload_schemaless

csv.field_size_limit(sys.maxsize)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    logger.info("Loaded %d records" % len(records))

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


def run(out_file,
        no_download=False,
        planning_file='',
        pts_file='',
        tco_file='',
        mohcd_pipeline_file='',
        mohcd_inclusionary_file='',
        affordable_file='',
        permit_addenda_file='',
        oewd_permits_file='',
        parcel_data_file='',
        diff=False,
        diff_file='',
        the_date=None,
        upload=False):

    sources = []
    dl_sources = {}
    destdir = tempfile.mkdtemp()
    client = get_client()
    with futures.ThreadPoolExecutor(
            thread_name_prefix="schemaless-download") as executor:
        for (source, arg) in [
                (Planning, planning_file),
                (PTS, pts_file),
                (TCO, tco_file),
                (MOHCDPipeline, mohcd_pipeline_file),
                (MOHCDInclusionary, mohcd_inclusionary_file),
                (PermitAddendaSummary, permit_addenda_file),
                (AffordableRentalPortfolio, affordable_file),
                (OEWDPermits, oewd_permits_file)]:
            if arg:
                sources.append(source(arg))
            elif source.DATA_SF_VIEW_ID and not no_download:
                dest = os.path.join(destdir, "%s.csv" % source.NAME)
                dl_sources[executor.submit(
                    download, client, source.DATA_SF_VIEW_ID, dest)] = source
            else:
                logger.warning("Skipping %s" % source.NAME)

        if not parcel_data_file:
            parcel_data_file_future = executor.submit(
                download,
                client,
                PARCELS_DATA_SF_VIEW_ID,
                os.path.join(destdir, 'parcels.csv'))
        if diff and not diff_file:
            diff_file_future = executor.submit(
                download,
                client,
                SCHEMALESS_VIEW_ID,
                os.path.join(destdir, 'schemaless-existing.csv'))

        for future in futures.as_completed(dl_sources):
            try:
                src = dl_sources[future]
                sources.append(src(future.result()))
            except Exception:
                logger.exception("Error downloading data for %s", src.NAME)
                raise
        if not parcel_data_file:
            parcel_data_file = parcel_data_file_future.result()
        if diff and not diff_file:
            diff_file = diff_file_future.result()

    if len(sources) == 0:
        parser.print_help()
        logger.error('ERROR: at least one source must be specified.')
        sys.exit(1)

    mapblklot_gen.init(parcel_data_file)

    if diff_file:
        dump_and_diff(sources, out_file, diff_file, the_date)
    else:
        just_dump(sources, out_file, the_date)

    if upload:
        upload_schemaless(out_file)


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

            To turn off automatic downloads, pass `--no_download True`.
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

    parser.add_argument('--diff', type=bool, default=False)
    parser.add_argument(
        '--diff_file',
        help='A schemaless csv generated by this script, to diff against.',
        default='')
    parser.add_argument(
        '--the-date',
        help=('Date this script was run, optional, defaults to today. '
              'YYYY-MM-DD'),
        default='')
    parser.add_argument('--parcel_data_file')
    parser.add_argument('--upload', type=bool, default=False)
    args = parser.parse_args()

    the_date = None
    if args.the_date:
        the_date = datetime.strptime(args.the_date, "%Y-%m-%d").date()

    run(args.out_file,
        no_download=args.no_download,
        planning_file=args.planning_file,
        pts_file=args.pts_file,
        tco_file=args.tco_file,
        mohcd_pipeline_file=args.mohcd_pipeline_file,
        mohcd_inclusionary_file=args.mohcd_inclusionary_file,
        affordable_file=args.affordable_file,
        permit_addenda_file=args.permit_addenda_file,
        oewd_permits_file=args.oewd_permits_file,
        parcel_data_file=args.parcel_data_file,
        diff=args.diff,
        diff_file=args.diff_file,
        the_date=args.the_date,
        upload=args.upload)
