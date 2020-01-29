# Lint as: python3
"""Convert a schemaless csv into relational tables (a set of csvs)."""
import argparse
from datetime import datetime
from collections import defaultdict
from collections import namedtuple
import csv
import lzma
import sys

csv.field_size_limit(sys.maxsize)

Field = namedtuple('Field',
                   ['name', 'value', 'always_treat_as_empty'],
                   defaults=['', '', False])


def gen_id(proj):
    return [Field('id', proj.id, True)]


def gen_facts(proj):
    result = [Field()] * 5

    if proj.field('address') != '':
        result[0] = Field('address', proj.field('address'))
        result[1] = Field('applicant', '')
        result[2] = Field('supervisor_district', '')
        result[3] = Field('permit_authority', 'planning')
        result[4] = Field('permit_authority_id', proj.field('fk'))

    return result


def gen_units(proj):
    result = [Field()] * 6

    # TODO: how to handle cases where better numbers exist from dbi
    # TODO: how to handle cases where prop - existing != net ?
    result[0] = Field('num_units', proj.field('market_rate_units_net'))
    if result[0].value != '':
        result[1] = Field('num_units_data', 'planning', True)

    result[2] = Field('num_units_bmr', proj.field('affordable_units_net'))
    if result[2].value != '':
        result[3] = Field('num_units_bmr_data', 'planning', True)

    result[4] = Field('num_square_feet', proj.field('residential_sq_ft_net'))
    if result[4].value != '':
        result[5] = Field('num_square_feet_data', 'planning', True)

    return result


def gen_geom(proj):
    result = [Field()] * 2  # TODO datafreshness

    if proj.field('the_geom') != '':
        result[0] = Field('name', 'geom')
        result[1] = Field('value', proj.field('the_geom'))

    return result


# Mapping of tables to a set of data generators.  All data generators must
# accept two arguments: project id, and a dict[source][fk][name] = value.
# They return a list of sequential columns of data to output.
config = {
    'project_facts': [
            gen_id,
            gen_facts,
            gen_units,
    ],
    'project_status_history': [
            # gen_id,
            # TODO
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
        projects = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(
                    lambda: defaultdict(
                        lambda: defaultdict(str)))))

        for line in reader:
            date = datetime.fromisoformat(line['last_updated'])
            value = line['value']

            id, src, fk, name = (
                line['id'], line['source'], line['fk'], line['name'])
            existing = projects[id][src][fk][name]

            if existing['value'] == '' or date > existing['last_updated']:
                existing['value'] = value
                existing['last_updated'] = date

            processed += 1
            if processed % 75000 == 0:
                print("Processed %s lines" % processed)

    return projects


Record = namedtuple('Record', ['key', 'values'], defaults=[None, None])


class Project:
    """A way to abstract some of the details of handling multiple records for a
    project, from multiple sources."""

    def __init__(self, id, data):
        self.id = id
        if len(data['ppts']) == 0:
            raise Exception('No implementation to handle non-ppts data yet')

        main = None
        children = []
        main_date = datetime.min
        for (fk, values) in data['ppts'].items():
            if values['parent']['value'] is None or values['parent']['value'] == '':
                if main is None or (
                        main is not None and
                        values['parent']['last_updated'] > main_date):
                    main =  Record(fk, values)
                    main_date = values['parent']['last_updated']
            else:
                if id == 'e23d90d3-fa75-40f6-957d-bf8e6626a37f':
                    print('Appending as child %s' % fk)
                children.append(Record(fk, values))

        if main is None:
            # upgrade the oldest child
            oldest_child_and_date = None
            for child in children:
                oldest_date = datetime.max
                for (name, data) in child.values.items():
                    if data['last_updated'] < oldest_date:
                        oldest_date = data['last_updated']

                if oldest_child_and_date is None or oldest_date < oldest_child_and_date[1]:
                    oldest_child_and_date = (child, oldest_date)

            if oldest_child_and_date is not None:
                main = oldest_child_and_date[0]
                children.remove(main)
            else:
                raise Exception('No main record found for a project %s' % id)

        self.__ppts_main = main
        self.__ppts_children = children

    @property
    def main(self):
        return self.__ppts_main

    @property
    def children(self):
        return self.__ppts_children

    def field(self, name):
        # for ppts, prefer parent record, only moving to children if none found,
        # at which point we choose the value with the latest last_updated
        # TODO: I'm not even sure this is the correct logic to use for dealing
        # with ambiguities.
        val = ''
        if name in self.__ppts_main.values:
            val = self.__ppts_main.values[name]['value']

        update_date = datetime.min
        if val == '':
            for child in self.__ppts_children:
                if name in child.values:
                    if child.values[name]['last_updated'] > update_date:
                        update_date = child.values[name]['last_updated']
                        val = child.values[name]['value']

        return val


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
                proj = Project(projectid, sources)
                output = []

                atleast_one = False
                for generator in generators:
                    results = generator(proj)
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
