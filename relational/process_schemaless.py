# Lint as: python3
"""Convert a schemaless csv into relational tables (a set of csvs)."""
import argparse
from concurrent import futures
from datetime import datetime
from collections import defaultdict
from collections import namedtuple
import csv
import logging
import lzma
import os
import pathlib
import queue
import sys
import tempfile

from fileutils import open_file
from datasf import download
from datasf import get_client
import relational.table as tabledef
from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.upload import upload_data_freshness
from relational.upload import upload_table
import schemaless.mapblklot_generator as mapblklot_gen
from schemaless.create_schemaless import SOCRATA_DATE_FORMAT
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import OEWDPermits
from schemaless.sources import PARCELS_DATA_SF_VIEW_ID
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Planning
from schemaless.sources import PTS
from schemaless.sources import TCO
from schemaless.sources import source_map
from schemaless.upload import SCHEMALESS_VIEW_ID
from schemaless.upload import UUID_VIEW_ID

csv.field_size_limit(min(2**31-1, sys.maxsize))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# TODO: ugly global state
__seen_ids = set()


def is_seen_id(row, table, seen_set):
    try:
        id_index = table.index(table.ID)
        return row[id_index] in seen_set
    except ValueError:
        return False


def store_seen_id(row, table, seen_set):
    try:
        id_index = table.index(table.ID)
        seen_set.add(row[id_index])
    except ValueError:
        pass


TableConfig = namedtuple('TableConfig',
                         ['table', 'pre_process', 'post_process'],
                         defaults=[None, None])

# Configure all tables to output; order is important so that we
# populate seen project IDs from ProjectFacts.
config = [
    tabledef.ProjectFacts(),
    tabledef.ProjectUnitCountsFull(),
    tabledef.ProjectCompletedUnitCounts(),
    tabledef.ProjectStatusHistory(),
    tabledef.ProjectGeo(),
    tabledef.ProjectDetails(),
]


class Freshness:
    _FIELD_SETS = {
        Planning.NAME: set(['date_opened', 'date_closed']),
        PTS.NAME: set([
            'completed_date',
            'current_status_date',
            'filed_date',
            'first_construction_document_date',
            'issued_date',
            'permit_creation_date',
        ]),
        TCO.NAME: set(['date_issued']),
    }

    def __init__(self):
        self.freshness = {}
        self.bad_dates = 0
        self.bad_dates_sample = {}
        self._freshness_checks = {
            Planning.NAME: self._planning,
            PTS.NAME: self._pts,
            TCO.NAME: self._tco,
            OEWDPermits.NAME: self._oewd_permits,
            MOHCDPipeline.NAME: self._mohcd_pipeline,
            MOHCDInclusionary.NAME: self._mohcd_inclusionary,
            AffordableRentalPortfolio.NAME: self._affordable_rental,
            PermitAddendaSummary.NAME: self._permit_addenda_summary,
        }

    def _check_and_log_good_date(self, date, source, line):
        if not date or date > datetime.today():
            self.bad_dates += 1
            if source not in self.bad_dates_sample:
                self.bad_dates_sample[source] = queue.Queue(maxsize=10)
            if not self.bad_dates_sample[source].full():
                self.bad_dates_sample[source].put_nowait(
                    '"%s" had a stored value of "%s" '
                    'and the schema-less was last updated: "%s"' % (
                        line.get('fk', ''),
                        line.get('value', ''),
                        line.get('last_updated', '')))
            return False
        return True

    def _extract_nv_date(self, line, source, timeformat='%m/%d/%Y'):
        if (line['source'] == source and
                line['name'] in self._FIELD_SETS[source]):
            nvdate = datetime.strptime(line['value'].split(' ')[0], timeformat)
            if not self._check_and_log_good_date(nvdate, source, line):
                return

            if (source not in self.freshness or
                    nvdate > self.freshness[source]):
                self.freshness[source] = nvdate

    def _extract_last_updated(self, line, source):
        if line['source'] == source:
            nvdate = datetime.strptime(
                line['last_updated'], SOCRATA_DATE_FORMAT)
            if not self._check_and_log_good_date(nvdate, source, line):
                return

            if (source not in self.freshness or
                    nvdate > self.freshness[source]):
                self.freshness[source] = nvdate

    def update_freshness(self, line):
        if line['source'] in self._freshness_checks:
            self._freshness_checks[line['source']](line)
        else:
            print('Warning: unknown source for '
                  'data freshness: %s, skipping' % line['source'])

    def _planning(self, line):
        self._extract_nv_date(line, Planning.NAME, timeformat='%Y-%m-%d')

    def _pts(self, line):
        self._extract_nv_date(line, PTS.NAME)

    def _tco(self, line):
        self._extract_nv_date(line, TCO.NAME, timeformat='%Y/%m/%d')

    def _oewd_permits(self, line):
        self._extract_last_updated(line, OEWDPermits.NAME)

    def _mohcd_pipeline(self, line):
        self._extract_last_updated(line, MOHCDPipeline.NAME)

    def _mohcd_inclusionary(self, line):
        self._extract_last_updated(line, MOHCDInclusionary.NAME)

    def _affordable_rental(self, line):
        self._extract_last_updated(line, AffordableRentalPortfolio.NAME)

    def _permit_addenda_summary(self, line):
        self._extract_last_updated(line, PermitAddendaSummary.NAME)


# entries_map is a dict of key, value of string=>list of Entry, where key is
#   the project uuid.
# freshness is a Freshness instance.
ProcessResult = namedtuple('ProcessResult', ['entries_map', 'freshness'])


def process_files(schemaless_file, uuid_mapping):
    """Consumes all data in the schemaless file to get the latest values.

    Returns: a ProcessResult
    """
    # TODO: for large schemaless files, this will fail with OOM.  We should
    # probably ensure a uuid sort order in the schemaless so we can batch
    # projects.

    if schemaless_file.endswith('.xz'):
        o = lzma.open
    else:
        o = open

    processed = 0
    with o(schemaless_file,
           mode='rt',
           encoding='utf-8',
           errors='replace') as inf:
        reader = csv.DictReader(inf)

        projects = defaultdict(list)
        freshness = Freshness()

        def _get_or_insert(id, fk, src):
            found_entry = None
            for entry in projects[id]:
                if entry.fk == fk and entry.source == src:
                    found_entry = entry
                    break

            if not found_entry:
                found_entry = Entry(fk, src, [])
                projects[id].append(found_entry)

            return found_entry

        for line in reader:
            date = datetime.strptime(line['last_updated'], SOCRATA_DATE_FORMAT)
            src, fk, name, value = (
                line['source'], line['fk'], line['name'], line['value'])
            id = uuid_mapping[fk]
            if not id:
                raise KeyError("Entry %s does not have a uuid" % fk)

            entry = _get_or_insert(id, fk, src)
            entry.add_name_value(NameValue(name, value, date))
            freshness.update_freshness(line)
            processed += 1
            if processed % 1000000 == 0:
                print('Processed %s lines' % processed)

        return ProcessResult(entries_map=projects, freshness=freshness)


def output_freshness(path, freshness):
    """Generates the table for indicating data freshness of sources."""
    with open(path, 'w') as outf:
        print('Handling %s' % path)
        writer = csv.writer(outf)
        writer.writerow(['source', 'freshness'])

        for (source, fresh_date) in freshness.freshness.items():
            out_source = source
            if source in source_map and hasattr(source_map[source], 'NAME'):
                out_source = source_map[source].NAME

            writer.writerow([out_source, fresh_date.strftime('%Y-%m-%d')])

    if freshness.bad_dates > 0:
        print('Found %s bad dates' % freshness.bad_dates)
        print('Sample entries:')
        for (source, bad_dates_queue) in freshness.bad_dates_sample.items():
            print('\tFor source "%s"' % source)
            while not bad_dates_queue.empty():
                sample = bad_dates_queue.get_nowait()
                print('\t\t%s' % sample)


def build_projects(entries_map, recordgraph):
    """Returns a list of Project"""
    projects = []
    bad_projects = 0
    bad_projects_sample = queue.Queue(maxsize=10)
    for (projectid, entries) in entries_map.items():
        try:
            projects.append(Project(projectid, entries, recordgraph))
            if len(projects) % 100000 == 0:
                print('Processed %s projects' % len(projects))
        except ValueError as err:
            bad_projects += 1
            if not bad_projects_sample.full():
                bad_projects_sample.put_nowait(err)

    if bad_projects > 0:
        print('Skipped %s projects due to problems. Samples below...' %
              bad_projects)
        while not bad_projects_sample.empty():
            print('\t%s' % bad_projects_sample.get_nowait())

    return projects


def output_projects(out_prefix, projects, config):
    """Generates the relational tables from the project info"""
    lines_out = 0
    for table in config:
        if lines_out > 0:
            print('\t%s total entries' % lines_out)
            lines_out = 0

        finalfile = pathlib.Path(out_prefix) / ("%s.csv" % table.name)
        with open(finalfile, 'w') as outf:
            print('Handling %s' % finalfile)
            headers_printed = False
            for proj in projects:
                writer = csv.writer(outf)

                output = []
                if (isinstance(table, tabledef.ProjectFacts) or
                        proj.id in tabledef.ProjectFacts.SEEN_IDS):
                    for row in table.rows(proj):
                        output.append(row)

                if len(output) > 0:
                    if not headers_printed:
                        writer.writerow(table.header())
                        headers_printed = True

                    for out in output:
                        lines_out += 1
                        if lines_out % 5000 == 0:
                            print('\t...%s entries to %s' %
                                  (lines_out, finalfile))
                        writer.writerow(out)
        table.log_bad_data()

    if lines_out > 0:
        print('\t%s total entries' % lines_out)


def build_uuid_mapping(uuid_map_file):
    mapping = {}
    with open_file(uuid_map_file, 'r') as f:
        reader = csv.DictReader(f)
        for line in reader:
            mapping[line['fk']] = line['uuid']
    return mapping


def run(schemaless_file='',
        uuid_map_file='',
        parcel_data_file='',
        out_prefix='',
        upload=False):
    destdir = tempfile.mkdtemp()
    if not out_prefix:
        out_prefix = destdir
    out_prefix = pathlib.Path(out_prefix)
    if not parcel_data_file or not schemaless_file or not uuid_map_file:
        with futures.ThreadPoolExecutor(
                thread_name_prefix="uuid-download") as executor:
            client = get_client()
            if not parcel_data_file:
                parcel_data_file_future = executor.submit(
                    download,
                    client,
                    PARCELS_DATA_SF_VIEW_ID,
                    os.path.join(destdir, 'parcels.csv'))
            if not schemaless_file:
                schemaless_file_future = executor.submit(
                    download,
                    client,
                    SCHEMALESS_VIEW_ID,
                    os.path.join(destdir, 'schemaless.csv'))
            if not uuid_map_file:
                uuid_map_file_future = executor.submit(
                    download,
                    client,
                    UUID_VIEW_ID,
                    os.path.join(destdir, 'uuid.csv'))
            if not parcel_data_file:
                parcel_data_file = parcel_data_file_future.result()
            if not schemaless_file:
                schemaless_file = schemaless_file_future.result()
            if not uuid_map_file:
                uuid_map_file = uuid_map_file_future.result()

    logger.info("Writing output to %s", out_prefix)
    # Make sure our output dir exists
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    mapblklot_gen.init(parcel_data_file)

    uuid_mapping = build_uuid_mapping(uuid_map_file)
    process_result = process_files(schemaless_file, uuid_mapping)

    print('Some stats:')
    print('\tnumber of projects: %s' % len(process_result.entries_map))

    entry_count = 0
    nv_count = 0

    for (id, entries) in process_result.entries_map.items():
        entry_count += len(entries)
        for entry in entries:
            nv_count += entry.num_name_values()
    print('\ttotal records rolled up: %s' % entry_count)
    print('\ttotal fields: %s' % nv_count)

    print('Building record graph...')
    rg = RecordGraph.from_files(schemaless_file, uuid_map_file)
    output_projects(
        out_prefix, build_projects(process_result.entries_map, rg), config)

    freshness_path = out_prefix / 'data_freshness.csv'
    output_freshness(freshness_path, process_result.freshness)

    if upload:
        jobs = {}
        with futures.ThreadPoolExecutor(
                thread_name_prefix="relational-upload") as executor:
            for table in config:
                path = out_prefix / ("%s.csv" % table.name)
                jobs[executor.submit(
                    upload_table, type(table), path)] = table.name
            jobs[executor.submit(
                upload_data_freshness, freshness_path)] = "freshness"

            for future in futures.as_completed(jobs):
                try:
                    output = jobs[future]
                    logger.info(future.result())
                    logger.info("Done uploading %s", output)
                except Exception:
                    logger.exception("Error uploading data for %s", output)
                    raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--schemaless_file',
        help='Schema-less CSV to use',
        default='')
    parser.add_argument(
        '--uuid_map_file',
        help='CSV that maps uuids to all seen fks',
        default='')
    parser.add_argument(
        '--out_prefix',
        help='Prefix for output files',
        default='')
    parser.add_argument('--parcel_data_file', help='Parcel data', default='')
    parser.add_argument('--upload', type=bool, default=False)
    args = parser.parse_args()

    run(schemaless_file=args.schemaless_file,
        uuid_map_file=args.uuid_map_file,
        parcel_data_file=args.parcel_data_file,
        out_prefix=args.out_prefix,
        upload=args.upload)
