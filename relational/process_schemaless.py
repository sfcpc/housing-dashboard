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
from schemaless.sources import MOHCD
from schemaless.sources import PPTS
from schemaless.sources import PTS

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


_FIELD_PREDICATE = {
    PPTS.NAME: set(['date_opened', 'date_closed']),
    PTS.NAME: set([
        'completed_date',
        'current_status_date',
        'filed_date',
        'first_construction_document_date',
        'issued_date',
        'permit_creation_date',
    ]),
    MOHCD.NAME: set([
        'date_issuance_of_building_permit',
        'date_issuance_of_first_construction_document',
        'date_issuance_of_notice_to_proceed',
    ]),
}


def extract_freshness(entries_map):
    """Extracts the last time a data source has been fetched

    Returns: a dict with key data source and value a datetime
    """
    data_freshness = {}
    bad_dates = 0
    bad_date_sample = queue.Queue(maxsize=10)
    for (projectid, entries) in entries_map.items():
        for entry in entries:
            if entry.source not in _FIELD_PREDICATE:
                print('Warning: unknown source for '
                      'data freshness: %s, skipping' % entry.source)
                continue

            if entry.source not in data_freshness:
                data_freshness[entry.source] = datetime.min

            for (name, value) in entry.latest_name_values().items():
                if name in _FIELD_PREDICATE[entry.source]:
                    nvdate = datetime.strptime(
                            value.split(' ')[0],
                            '%m/%d/%Y')
                    if not nvdate or nvdate > datetime.today():
                        bad_dates += 1
                        if not bad_date_sample.full():
                            bad_date_sample.put_nowait(
                                '"%s" for %s' % (value, entry.fk))
                        continue

                    if nvdate > data_freshness[entry.source]:
                        data_freshness[entry.source] = nvdate

    print('Found %s bad dates' % bad_dates)
    print('Sample entries:')
    while not bad_date_sample.empty():
        sample = bad_date_sample.get_nowait()
        print('\t%s' % sample)

    return data_freshness


def build_entries_map(schemaless_file, uuid_mapping):
    """Consumes all data in the schemaless file to get the latest values.

    Returns: a dict with key project uuid and value a list of Entry
    """
    # TODO: for large schemaless files, this will fail with OOM.  We should
    # probably ensure a uuid sort order in the schemaless so we can batch
    # projects.
    # TODO: don't use so many nested dicts and have a bit more structure,
    # would make handling data from multiple sources and fks easier

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
            processed += 1
            if processed % 1000000 == 0:
                print('Processed %s lines' % processed)

    return projects


def output_freshness(freshness):
    """Generates the table for indicating data freshness of sources."""
    finalfile = args.out_prefix + 'data_freshness.csv'
    with open(finalfile, 'w') as outf:
        print('Handling %s' % finalfile)
        writer = csv.writer(outf)
        writer.writerow(['source', 'freshness'])

        for (source, freshness) in freshness.items():
            out_source = source

            # TODO: for multiple sources, have a better way to normalize this
            if out_source == 'ppts':
                out_source = 'planning'

            writer.writerow([out_source, freshness.strftime('%Y-%m-%d')])


def build_projects(entries_map, recordgraph):
    """Returns a list of Project"""
    projects = []
    projects_built = 0
    for (projectid, entries) in entries_map.items():
        projects.append(Project(projectid, entries, recordgraph))
        projects_built += 1
        if projects_built % 100000 == 0:
            print('Processed %s projects' % projects_built)

    return projects


def output_projects(projects, config):
    """Generates the relational tables from the project info"""

    lines_out = 0
    for table in config:
        if lines_out > 0:
            print("%s total entries" % lines_out)
            lines_out = 0

        finalfile = args.out_prefix + table.name + ".csv"
        with open(finalfile, 'w') as outf:
            print("Handling %s" % finalfile)
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
                            print("%s entries to %s" % (lines_out, finalfile))
                        writer.writerow(out)

    if lines_out > 0:
        print("%s total entries" % lines_out)


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
    entries_map = build_entries_map(args.schemaless_file, uuid_mapping)

    print("Some stats:")
    print("\tnumber of projects: %s" % len(entries_map))

    entry_count = 0
    nv_count = 0

    for (id, entries) in entries_map.items():
        entry_count += len(entries)
        for entry in entries:
            nv_count += entry.num_name_values()
    print("\ttotal records rolled up: %s" % entry_count)
    print("\ttotal fields: %s" % nv_count)

    rg = RecordGraph.from_files(args.schemaless_file, args.uuid_map_file)
    output_projects(build_projects(entries_map, rg), config)
    freshness = extract_freshness(entries_map)
    output_freshness(freshness)
