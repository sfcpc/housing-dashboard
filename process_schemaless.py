# Lint as: python3
"""Convert a schemaless csv into relational tables (a set of csvs)."""
import argparse
from datetime import datetime
from collections import OrderedDict
from collections import defaultdict
from collections import namedtuple
import csv
import lzma
import sys

from fileutils import open_file

from process.project import Project
from process.generators import gen_id
from process.generators import gen_facts
from process.generators import gen_units
from process.generators import nv_geom
from process.generators import nv_all_units
from process.generators import nv_square_feet
from process.generators import atleast_one_measure
from process.types import four_level_dict

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
#   of NameValue.
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
        ],
        addl_output_predicate=lambda r, h: is_seen_id(r, h, __seen_ids),
    )),
])


# TODO data freshness table, which is not on a per-project basis


def build_projects(schemaless_file, uuid_mapping):
    """Consumes all data in the schemaless file to get the latest values.

    Returns: a nested dict keyed as:
        dict[id][source][name] = {
            value: '',
            last_updated: datetime,
        }
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

        # five levels of dict
        projects = defaultdict(lambda: four_level_dict())

        for line in reader:
            date = datetime.fromisoformat(line['last_updated'])
            value = line['value']

            src, fk, name = (
                line['source'], line['fk'], line['name'])
            id = uuid_mapping[fk]
            if not id:
                raise KeyError("Entry %s does not have a uuid" % fk)

            existing = projects[id][src][fk][name]

            if existing['value'] == '' or date > existing['last_updated']:
                existing['value'] = value
                existing['last_updated'] = date

            processed += 1
            if processed % 500000 == 0:
                print("Processed %s lines" % processed)

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
            for (projectid, sources) in projects.items():
                writer = csv.writer(outf)
                proj = Project(projectid, sources)
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

    projects = build_projects(args.schemaless_file, uuid_mapping)

    print("Some stats:")
    print("\tnumber of projects: %s" % len(projects))

    source_count = defaultdict(int)
    fk_count = 0
    nv_count = 0
    est_bytes = 0
    for (id, sources) in projects.items():
        for (source, entries) in sources.items():
            source_count[source] += 1
            fk_count += len(entries)
            for (entry, namevalue) in entries.items():
                nv_count += len(namevalue)
                for (name, value) in namevalue.items():
                    est_bytes += sys.getsizeof(value)
    print("\tsource counts: %s" % source_count)
    print("\ttotal records rolled up: %s" % fk_count)
    print("\ttotal fields: %s" % nv_count)
    print("\test bytes for values: %s" % est_bytes)

    output_projects(projects, config)
