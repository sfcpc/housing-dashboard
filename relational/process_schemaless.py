# Lint as: python3
"""Convert a schemaless csv into relational tables (a set of csvs)."""
import argparse
from datetime import datetime
from collections import defaultdict
from collections import namedtuple
import csv
import lzma
import queue
import sys

from fileutils import open_file

import relational.table as tabledef
from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import MOHCDPipeline
from schemaless.sources import PPTS
from schemaless.sources import PTS
from schemaless.sources import source_map

csv.field_size_limit(sys.maxsize)


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
    # TODO: ProjectStatusHistory
    tabledef.ProjectGeo(),
    tabledef.ProjectDetails(),
]


class Freshness:
    _FIELD_SETS = {
        PPTS.NAME: set(['date_opened', 'date_closed']),
        PTS.NAME: set([
            'completed_date',
            'current_status_date',
            'filed_date',
            'first_construction_document_date',
            'issued_date',
            'permit_creation_date',
        ]),
    }

    def __init__(self):
        self.freshness = {}
        self.bad_dates = 0
        self.bad_dates_sample = {}
        self._freshness_checks = {
            PPTS.NAME: self._ppts,
            PTS.NAME: self._pts,
            MOHCDPipeline.NAME: self._mohcd,
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

    def _extract_nv_date(self, line, source):
        if (line['source'] == source and
                line['name'] in self._FIELD_SETS[source]):
            nvdate = datetime.strptime(line['value'].split(' ')[0], '%m/%d/%Y')
            if not self._check_and_log_good_date(nvdate, source, line):
                return

            if (source not in self.freshness or
                    nvdate > self.freshness[source]):
                self.freshness[source] = nvdate

    def _extract_last_updated(self, line, source):
        if line['source'] == source:
            nvdate = datetime.fromisoformat(line['last_updated'])
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

    def _ppts(self, line):
        self._extract_nv_date(line, PPTS.NAME)

    def _pts(self, line):
        self._extract_nv_date(line, PTS.NAME)

    def _mohcd(self, line):
        self._extract_last_updated(line, MOHCDPipeline.NAME)


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
            date = datetime.fromisoformat(line['last_updated'])
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


def output_freshness(freshness):
    """Generates the table for indicating data freshness of sources."""
    finalfile = args.out_prefix + 'data_freshness.csv'
    with open(finalfile, 'w') as outf:
        print('Handling %s' % finalfile)
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
    for (projectid, entries) in entries_map.items():
        projects.append(Project(projectid, entries, recordgraph))
        if len(projects) % 100000 == 0:
            print('Processed %s projects' % len(projects))

    return projects


def output_projects(projects, config):
    """Generates the relational tables from the project info"""

    lines_out = 0
    for table in config:
        if lines_out > 0:
            print('%s total entries' % lines_out)
            lines_out = 0

        finalfile = args.out_prefix + table.name + '.csv'
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
                        if lines_out % 10000 == 0:
                            print('%s entries to %s' % (lines_out, finalfile))
                        writer.writerow(out)

    if lines_out > 0:
        print('%s total entries' % lines_out)


def build_uuid_mapping(uuid_map_file):
    mapping = {}
    with open_file(uuid_map_file, 'r') as f:
        reader = csv.DictReader(f)
        for line in reader:
            mapping[line['fk']] = line['uuid']
    return mapping


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('schemaless_file', help='Schema-less CSV to use')
    parser.add_argument('uuid_map_file',
                        help='CSV that maps uuids to all seen fks')
    parser.add_argument(
            '--out_prefix',
            help='Prefix for output files',
            default='')
    args = parser.parse_args()

    uuid_mapping = build_uuid_mapping(args.uuid_map_file)
    process_result = process_files(args.schemaless_file, uuid_mapping)

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
    rg = RecordGraph.from_files(args.schemaless_file, args.uuid_map_file)
    output_projects(build_projects(process_result.entries_map, rg), config)
    output_freshness(process_result.freshness)
