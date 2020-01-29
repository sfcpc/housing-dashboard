# Lint as: python3
"""Convert a schemaless csv into relational tables (a set of csvs)."""
import argparse
from collections import defaultdict
from collections import namedtuple
import csv
import lzma
import sys

csv.field_size_limit(sys.maxsize)

Field = namedtuple('Field',
                   ['name', 'value', 'always_treat_as_empty'],
                   defaults=['', '', False])


def gen_id(id, data):
    return [Field('id', id, True)]


def gen_facts(id, data):
    result = [Field()] * 5

    if len(data['ppts']) == 0:
        return result

    for (fk, values) in data['ppts'].items():
        if values['address'] == '':
            continue

        result[0] = Field('address', values['address'])
        result[1] = Field('applicant', '')
        result[2] = Field('supervisor_district', '')
        result[3] = Field('permit_authority', values['ppts'])
        result[4] = Field('permit_authority_id', fk)
        break
    return result


def gen_geom(id, data):
    result = [Field()] * 2  # TODO datafreshness

    if len(data['ppts']) == 0:
        return result

    for (fk, values) in data['ppts'].items():
        if values['the_geom'] == '':
            continue

        result[0] = Field('name', 'geom')
        result[1] = Field('value', values['the_geom'])
        break

    return result


# Mapping of tables to a set of data generators.  All data generators must
# accept two arguments: project id, and a dict[source][fk][name] = value.
# They return a list of sequential columns of data to output.
config = {
    'project_facts': [
            gen_id,
            gen_facts,
    ],
    'project_status_history': [
            # TODO
            # gen_id,
    ],
    'project_geo': [
            gen_id,
            gen_geom,
            # TODO blocklot
    ],
    'project_details': [
            # gen_id,
    ],
}


# TODO data freshness table, which is not on a per-project basis


def build_projects(schemaless_file):
    """Consumes all data in the schemaless file to get the latest values.

    Returns: a nested dict keyed as:
        dict[id][source][name] = value
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

        # four levels of dict
        projects = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(
                    lambda: defaultdict(str))))

        for line in reader:
            # TODO: data-integrity validation
            record = projects[line['id']][line['source']]
            record[line['fk']][line['name']] = line['value']
            processed += 1
            if processed % 50000 == 0:
                print("Processed %s lines" % processed)

    return projects


def output_projects(projects, config):
    """Generates the relational tables from the project info"""

    lines_out = 0
    for (outfile, generators) in config.items():
        if lines_out > 0:
            print("%s total entries" % lines_out)
            lines_out = 0

        finalfile = args.out_prefix + outfile + ".csv"
        with open(finalfile, 'w') as outf:
            print("Handling %s" % finalfile)
            headers = []
            headers_printed = False
            for (projectid, sources) in projects.items():
                writer = csv.writer(outf)
                output = []

                atleast_one = False
                for generator in generators:
                    results = generator(projectid, sources)
                    for result in results:
                        if not headers_printed:
                            headers.append(result.name)
                        if (not result.always_treat_as_empty and
                                result.value != ""):
                            atleast_one = True
                        output.append(result.value)

                if atleast_one:
                    if not headers_printed and len(headers) > 0:
                        writer.writerow(headers)
                        headers_printed = True

                    lines_out += 1
                    if lines_out % 10000 == 0:
                        print("%s entries to %s" % (lines_out, finalfile))

                    writer.writerow(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('schemaless_file', help='Schema-less CSV to use')
    parser.add_argument(
            '--out_prefix',
            help='Prefix for output files',
            default='')
    args = parser.parse_args()

    projects = build_projects(args.schemaless_file)

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
