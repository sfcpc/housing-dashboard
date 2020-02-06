# Lint as: python3
"""Convert a schemaless csv into relational tables (a set of csvs)."""
import argparse
from datetime import datetime
from collections import OrderedDict
from collections import defaultdict
from collections import namedtuple
import csv
import lzma
import queue
import sys

from fileutils import open_file

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.generators import gen_id
from relational.generators import gen_facts
from relational.generators import gen_units
from relational.generators import nv_geom
from relational.generators import nv_all_units
from relational.generators import nv_bedroom_info
from relational.generators import nv_square_feet
from relational.generators import atleast_one_measure
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import MOHCD
from schemaless.sources import PPTS
from schemaless.sources import PTS

csv.field_size_limit(sys.maxsize)


# TODO: ugly global state
__seen_ids = set()


def is_seen_id(row, header, seen_set):
    try:
        id_index = header.index('id')
        return row[id_index] in seen_set
    except ValueError:
        return False


def store_seen_id(row, header, seen_set):
    try:
        id_index = header.index('id')
        seen_set.add(row[id_index])
    except ValueError:
        pass


TableDefinition = namedtuple('TableDefinition',
                             ['data_generators',
                              'name_value_generators',
                              'addl_output_predicate',
                              'post_process'],
                             defaults=[[], [], None, None])

# Mapping of tables to a set of data generators.
# * All data generators must accept a Project and return a list<string>.
# * All name value generators must accept a Project and return a list
#   of OutputNameValue.
# * For a combination of data generators and name value, all data returned
#   by data generators will be duplicated for each name value.
config = OrderedDict([
    ('project_facts', TableDefinition(
        data_generators=[
            gen_id,
            gen_facts,
            gen_units,
        ],
        addl_output_predicate=atleast_one_measure,
        post_process=lambda r, h: store_seen_id(r, h, __seen_ids),
    )),
    ('project_unit_counts_full', TableDefinition(
        data_generators=[
            gen_id,
        ],
        name_value_generators=[
            nv_all_units,
        ],
        addl_output_predicate=lambda r, h: is_seen_id(r, h, __seen_ids),
    )),
    ('project_status_history', TableDefinition(
        # TODO
    )),
    ('project_geo', TableDefinition(
        data_generators=[
            gen_id,
        ],
        name_value_generators=[
            nv_geom,
        ],
        addl_output_predicate=lambda r, h: is_seen_id(r, h, __seen_ids),
    )),
    ('project_details', TableDefinition(
        data_generators=[
            gen_id,
        ],
        name_value_generators=[
            nv_square_feet,
            nv_bedroom_info,
        ],
        addl_output_predicate=lambda r, h: is_seen_id(r, h, __seen_ids),
    )),
])


_FIELD_PREDICATE = {
    PPTS.NAME: set(['date_opened', 'date_closed']),
    PTS.NAME: set([
        'completed_date',
        'current_status_date',
        'filed_date',
        'first_construction_document_date',
        'issued_date',
        'permit_creation_date',
        'permit_expiration_date',
    ]),
    MOHCD.NAME: set([
        'date_estimated_construction_completion',
        'date_estimated_or_actual_actual_construction_start',
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
            if processed % 500000 == 0:
                print("Processed %s lines" % processed)

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
    for (projectid, entries) in entries_map.items():
        projects.append(Project(projectid, entries, recordgraph))

    return projects


def output_projects(projects, config):
    """Generates the relational tables from the project info"""

    lines_out = 0
    for (outfile, table_def) in config.items():
        if lines_out > 0:
            print("%s total entries" % lines_out)
            lines_out = 0

        finalfile = args.out_prefix + outfile + ".csv"
        with open(finalfile, 'w') as outf:
            print("Handling %s" % finalfile)
            headers_printed = False
            headers_done = False
            headers = []
            for proj in projects:
                writer = csv.writer(outf)
                output = []

                atleast_one = False
                for generator in table_def.data_generators:
                    results = generator(proj)
                    for result in results:
                        if not headers_done:
                            headers.append(result.name)
                        if (not result.always_treat_as_empty and
                                result.value != ""):
                            atleast_one = True
                        output.append(result.value)

                nvs = []
                if len(table_def.name_value_generators) > 0:
                    if not headers_done:
                        headers.extend(['name', 'value', 'data_source'])
                    for name_value in table_def.name_value_generators:
                        nvs.extend(name_value(proj))

                final_output = [output]
                if len(nvs) > 0:
                    final_output = []
                    for nv in nvs:
                        final_output.append(output +
                                            [nv.name, nv.value,
                                             nv.data_source])

                    if len(final_output) > 0:
                        atleast_one = True

                headers_done = True

                if atleast_one:
                    for out in final_output:
                        if (not table_def.addl_output_predicate or
                                table_def.addl_output_predicate(out, headers)):
                            if not headers_printed and len(headers) > 0:
                                writer.writerow(headers)
                                headers_printed = True

                            lines_out += 1
                            if lines_out % 10000 == 0:
                                print("%s entries to %s" % (lines_out,
                                                            finalfile))

                            writer.writerow(out)

                            if table_def.post_process:
                                table_def.post_process(out, headers)
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
