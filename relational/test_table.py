# Lint as: python3
from datetime import datetime
from collections import namedtuple

import pytest

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.table import ProjectDetails
from relational.table import ProjectFacts
from relational.table import ProjectGeo
from relational.table import ProjectStatusHistory
from relational.table import ProjectCompletedUnitCounts
from relational.table import ProjectUnitCountsFull
from schemaless.create_uuid_map import Node
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import OEWDPermits
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Planning
from schemaless.sources import PTS
from schemaless.sources import TCO


def _get_value_for_row(table, rows, name, return_multiple=False):
    if len(rows) > 1:
        raise ValueError('_get_value_for_row expected a one-row result')
    elif len(rows) == 0:
        return ''
    return rows[0][table.index(name)]


def _get_value_for_name(table, rows, name, return_multiple=False):
    result = []
    for row in rows:
        row_name = row[table.index(table.NAME)]
        if row_name == name:
            row_value = row[table.index(table.VALUE)]
            if return_multiple:
                result.append(row_value)
            else:
                return row_value
    return '' if not return_multiple else sorted(result, key=int)


@pytest.fixture
def d():
    """A default date to use for entries"""
    return datetime.fromisoformat('2019-01-01')


@pytest.fixture
def basic_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    rg.add(Node(record_id='2', parents=['1']))
    rg.add(Node(record_id='3', parents=['1']))
    rg.add(Node(record_id='4', parents=['1']))
    rg.add(Node(record_id='5', parents=['1']))
    rg.add(Node(record_id='6', parents=['1']))
    return rg


EntriesTestRow = namedtuple('EntriesTestRow', ['name', 'entries', 'want'])


def test_table_project_facts_atleast_one_measure():
    table = ProjectFacts()

    RowTest = namedtuple('RowTest', ['input', 'want', 'name'])
    tests = [
        RowTest(['', '', ''], False, 'empty row'),
        RowTest(['', '0', ''], False, 'zero different from empty'),
        RowTest(['1', '2', ''], True, 'normal full row'),
        RowTest(['', '', '1'], True, 'estimated field'),
    ]
    for test in tests:
        row = [''] * len(table.header())
        row[table.index(table.NET_NUM_UNITS)] = test.input[0]
        row[table.index(table.NET_NUM_UNITS_BMR)] = test.input[1]
        row[table.index(table.NET_EST_NUM_UNITS_BMR)] = test.input[2]
        assert table._atleast_one_measure(row) == test.want, test.name


def test_table_project_facts(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='use name if no address',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_units', '10', d)]),
            ],
            want={
                'name': 'BALBOA RESERVOIR DEVELOPMENT',
                'address': '',
            }),
        EntriesTestRow(
            name='supervisor district and developer org',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('supervisor_district', '1', d),
                       NameValue('developer_org', 'abc', d)]),
            ],
            want={
                'supervisor_district': '1',
                'applicant': 'abc',
            }),
        EntriesTestRow(
            name='developer name',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('supervisor_district', '1', d),
                       NameValue('developer_name', 'xyz', d)]),
            ],
            want={
                'applicant': 'xyz',
            }),
        EntriesTestRow(
            name='exclude no-address planning entries if no net units',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_units', '0', d)]),
            ],
            want={
                'name': '',
                'address': '',
            }),
        EntriesTestRow(
            name='use address for name if no name',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('address', '123 chris st', d),
                       NameValue('number_of_units', '10', d)]),
            ],
            want={
                'name': '123 chris st',
                'address': '123 chris st',
            }),
        EntriesTestRow(
            name='strip out planning zip code for name if found',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('address', '123 chris st 94114', d),
                       NameValue('number_of_units', '10', d)]),
            ],
            want={
                'name': '123 chris st',
                'address': '123 chris st 94114',
            }),
        EntriesTestRow(
            name='always use mohcd if information found',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_units', '10', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', '1', d),
                       NameValue('project_name', 'BALBOA!', d),
                       NameValue('street_number', '123', d),
                       NameValue('street_name', 'chris', d),
                       NameValue('street_type', 'st', d),
                       NameValue('zip_code', '94123', d)]),
            ],
            want={
                'name': 'BALBOA!',
                'address': '123 chris st, 94123',
            }),
        EntriesTestRow(
            name='use mohcd subset for name if no name found',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', '1', d),
                       NameValue('street_number', '123', d),
                       NameValue('street_name', 'chris', d),
                       NameValue('street_type', 'st', d),
                       NameValue('zip_code', '94123', d)]),
            ],
            want={
                'name': '123 chris st',
                'address': '123 chris st, 94123',
            }),
        EntriesTestRow(
            name='override planning name with mohcd name if found',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_units', '10', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', '1', d),
                       NameValue('project_name', 'chris place', d),
                       NameValue('street_number', '123', d),
                       NameValue('street_name', 'chris', d),
                       NameValue('street_type', 'st', d),
                       NameValue('zip_code', '94123', d)]),
            ],
            want={
                'name': 'chris place',
                'address': '123 chris st, 94123',
            }),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(
                table,
                fields,
                name) == wantvalue, ('%s, field: %s' % (test.name, name))


def test_table_project_facts_units(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='simple test',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '1', d),
                       NameValue('existing_units', '7', d),
                       NameValue('proposed_units', '5', d)]),
            ],
            want='-2'),
        EntriesTestRow(
            name='get from Planning because PTS data is incomplete',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('proposed_units', '7', d)]),
            ],
            want='10'),
        EntriesTestRow(
            name='get from Planning because PTS data has no proposed units',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '1', d),
                       NameValue('existing_units', '7', d)]),
            ],
            want='10'),
        EntriesTestRow(
            name='get from PTS because we can infer from just proposed',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '1', d),
                       NameValue('proposed_units', '7', d)]),
            ],
            want='7'),
        EntriesTestRow(
            name='get from Planning because no other choice',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
            ],
            want='10'),
        EntriesTestRow(
            name='get from PTS because permit_type 3 is also valid',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '3', d),
                       NameValue('proposed_units', '7', d)]),
            ],
            want='7'),
        EntriesTestRow(
            name='sum up across PTS records, ignoring dupes',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '7', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('permit_type', '1', d),
                       NameValue('proposed_units', '8', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('permit_type', '1', d),
                       NameValue('proposed_units', '8', d)]),
            ],
            want='15'),
        EntriesTestRow(
            name='sum up across PTS records, ignoring withdrawn/cancelled',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '7', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('current_status', 'withdrawn', d),
                       NameValue('permit_type', '1', d),
                       NameValue('proposed_units', '8', d)]),
                Entry('4',
                      PTS.NAME,
                      [NameValue('current_status', 'cancelled', d),
                       NameValue('permit_type', '1', d),
                       NameValue('proposed_units', '8', d)]),
            ],
            want='7'),
        EntriesTestRow(
            name='dont use 0 pts unit count',
            entries=[
                Entry('1',
                      Planning.NAME,
                      []),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '0', d)]),
            ],
            want=''),
        EntriesTestRow(
            name='dont use 0 planning unit',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '0', d)]),
            ],
            want=''),
        EntriesTestRow(
            name='dont use 0 even if in planning and pts',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '0', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '0', d)]),
            ],
            want=''),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        assert _get_value_for_row(table,
                                  fields,
                                  'net_num_units') == test.want, \
            'Failed "%s"' % test.name


def test_table_project_facts_da_prefer_planning(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='get from planning instead of dbi because da',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '200', d),
                       NameValue('record_type', 'PRJ', d)]),
                Entry('2',
                      Planning.NAME,
                      [NameValue('record_type', 'PHA', d),
                       NameValue('record_id', '123', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '10', d)]),
            ],
            want={
                'net_num_units': '200',
            },
        ),
        EntriesTestRow(
            name='get from dbi since da but unit counts close to planning',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '200', d),
                       NameValue('record_type', 'PRJ', d)]),
                Entry('2',
                      Planning.NAME,
                      [NameValue('record_type', 'PHA', d),
                       NameValue('record_id', '123', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '190', d)]),
            ],
            want={
                'net_num_units': '190',
            },
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_units_planning_bmr(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='simple bmr calculation',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '20', d),
                       NameValue('number_of_affordable_units', '10', d)]),
            ],
            want={'net_num_units_bmr': '10',
                  'net_estimated_num_units_bmr': ''},
        ),
        EntriesTestRow(
            name='estimated bmr calculation for medium project',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '20', d)]),
            ],
            want={'net_num_units_bmr': '',
                  'net_estimated_num_units_bmr': '4'},
        ),
        EntriesTestRow(
            name='estimated bmr calculation for large project',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '30', d)]),
            ],
            want={'net_num_units_bmr': '',
                  'net_estimated_num_units_bmr': '9'},
        ),
        EntriesTestRow(
            name='estimated bmr calculation for small project',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '5', d)]),
            ],
            want={'net_num_units_bmr': '',
                  'net_estimated_num_units_bmr': '0'},
        ),
        EntriesTestRow(
            name='estimated bmr calculation for medium project when zero',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '20', d),
                       NameValue('number_of_affordable_units', '0', d)]),
            ],
            want={'net_num_units_bmr': '',
                  'net_estimated_num_units_bmr': '4'},
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_units_ocii(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='get from OEWD because higher priority over pts',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      OEWDPermits.NAME,
                      [NameValue('total_units', '7', d),
                       NameValue('affordable_units', '1', d),
                       NameValue('delivery_agency', 'OCII', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '6', d)]),
            ],
            want={
                'net_num_units': '7',
                'net_num_units_bmr': '1',
                'net_num_units_data': OEWDPermits.NAME,
                'net_num_units_bmr_data': OEWDPermits.NAME,
            },
        ),
        EntriesTestRow(
            name='get from OEWD if total units of OEWD fields is filled',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      OEWDPermits.NAME,
                      [NameValue('total_units', '2', d),
                       NameValue('delivery_agency', 'OCII', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('permit_type', '2', d),
                       NameValue('proposed_units', '6', d)]),
            ],
            want={
                'net_num_units': '2',
                'net_num_units_bmr': '0',
                'net_num_units_data': OEWDPermits.NAME,
                'net_num_units_bmr_data': OEWDPermits.NAME,
            },
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_units_mohcd(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='get from pipeline because high priority over inclusionary',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('total_project_units', '7', d),
                       NameValue('total_affordable_units', '1', d)]),
                Entry('3',
                      MOHCDInclusionary.NAME,
                      [NameValue('total_project_units', '6', d),
                       NameValue('total_affordable_units', '2', d)]),
            ],
            want={
                'net_num_units': '7',
                'net_num_units_bmr': '1',
                'net_num_units_data': MOHCDPipeline.NAME,
                'net_num_units_bmr_data': MOHCDPipeline.NAME,
            },
        ),
        EntriesTestRow(
            name='get from inclusionary because no other choice',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('3',
                      MOHCDInclusionary.NAME,
                      [NameValue('total_project_units', '6', d),
                       NameValue('total_affordable_units', '2', d)]),
            ],
            want={
                'net_num_units': '6',
                'net_num_units_bmr': '2',
                'net_num_units_data': MOHCDInclusionary.NAME,
                'net_num_units_bmr_data': MOHCDInclusionary.NAME,
            },
        ),
        EntriesTestRow(
            name='no complete data set, but go with pipeline (do not merge '
                 'datasets)',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_units', '10', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('total_project_units', '7', d)]),
                Entry('3',
                      MOHCDInclusionary.NAME,
                      [NameValue('total_affordable_units', '2', d)]),
            ],
            want={
                'net_num_units': '7',
                'net_num_units_bmr': '0',
                'net_num_units_data': MOHCDPipeline.NAME,
                'net_num_units_bmr_data': MOHCDPipeline.NAME,
            },
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_invalid_prj(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='invalid PRJ',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d),
                       NameValue('status', 'withdrawn', d)]),
            ],
            want=True,
        ),
        EntriesTestRow(
            name='valid PRJ',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d),
                       NameValue('status', 'open', d)]),
            ],
            want=False,
        ),
        EntriesTestRow(
            name='valid PRJ and invalid PRJ',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d),
                       NameValue('status', 'withdrawn', d)]),
                Entry('3',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d),
                       NameValue('status', 'open', d)]),
            ],
            want=False,
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields_empty = len(table.rows(proj)) == 0

        assert fields_empty == test.want


def test_table_project_facts_pim_link(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='existing prj id',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d)]),
            ],
            want={'pim_link': 'https://sfplanninggis.org/pim?search=abc'},
        ),
        EntriesTestRow(
            name='no prj id, only block lot',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('mapblocklot', '2000', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_id', 'abc', d),
                       NameValue('record_type', 'CUA', d)]),
            ],
            want={'pim_link': 'https://sfplanninggis.org/pim?search=2000'},
        ),
        EntriesTestRow(
            name='no planning info, only pts info',
            entries=[
                Entry('1',
                      PTS.NAME,
                      [NameValue('block', '123', d),
                       NameValue('lot', 'ABC', d),
                       NameValue('permit_type', '1', d),
                       NameValue('proposed_units', '2', d)]),
                Entry('2',
                      OEWDPermits.NAME,
                      [NameValue('permit_number', '123', d),
                       NameValue('delivery_agency', 'OCII', d)]),
            ],
            want={'pim_link': 'https://sfplanninggis.org/pim?search=123ABC'},
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)
        print(fields)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_permit_authority(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='prj root',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d)]),
            ],
            want={'permit_authority': 'planning',
                  'permit_authority_id': 'abc'},
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)
        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_planner(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='planner',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d),
                       NameValue('assigned_to_planner', 'abc', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('block', '123', d),
                       NameValue('lot', 'ABC', d),
                       NameValue('permit_type', '1', d),
                       NameValue('proposed_units', '2', d)]),
            ],
            want={'planner': 'abc'},
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)
        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_bldg_authority(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='prj root',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('record_id', 'abc', d),
                       NameValue('number_of_units', '10', d),
                       NameValue('record_type', 'PRJ', d)]),
                Entry('2',
                      PTS.NAME,
                      [NameValue('permit_number', '123', d),
                       NameValue('permit_type', '1', d)]),
                Entry('3',
                      PTS.NAME,
                      [NameValue('permit_number', '456', d),
                       NameValue('permit_type', '1', d)]),
            ],
            want={'building_permit_authority_id': '123,456',
                  'building_permit_authority': 'dbi'},
        ),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)
        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_geo_dbi_location(basic_graph, d):
    table = ProjectGeo()

    lat_lng_str = '(37.7838191971246°, -122.41900657563552°)'
    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_id', 'abc', d),
               NameValue('number_of_units', '10', d),
               NameValue('record_type', 'PRJ', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('location', lat_lng_str, d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj_normal)
    assert _get_value_for_name(table, nvs, 'lat') == \
        '37.7838191971246'
    assert _get_value_for_name(table, nvs, 'lng') == \
        '-122.41900657563552'


def test_table_project_units_full_count(basic_graph, d):
    table = ProjectUnitCountsFull()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('number_of_units', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '5', d)]),
        Entry('2',  # Ignore duplicates from PTS
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '5', d)]),
        Entry('3',
              MOHCDPipeline.NAME,
              [NameValue('total_project_units', '7', d)]),
        Entry('4',
              MOHCDInclusionary.NAME,
              [NameValue('total_affordable_units', '5', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '6', d),
               NameValue('proposed_units', '5', d)]),
        Entry('6',
              OEWDPermits.NAME,
              [NameValue('total_units', '8', d),
               NameValue('affordable_units', '3', d),
               NameValue('delivery_agency', 'OCII', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj_normal)

    net_num_units = _get_value_for_name(
        table, nvs, 'net_num_units', return_multiple=True)
    assert len(net_num_units) == 5
    assert net_num_units[0] == '-3'
    assert net_num_units[1] == '0'
    assert net_num_units[2] == '7'
    assert net_num_units[3] == '8'
    assert net_num_units[4] == '10'

    net_num_bmr = _get_value_for_name(
        table, nvs, 'net_num_units_bmr', return_multiple=True)
    assert len(net_num_bmr) == 3  # only inferrable data is in MOHCD or OEWD
    assert net_num_bmr[0] == '0'
    assert net_num_bmr[1] == '3'
    assert net_num_bmr[2] == '5'

    # No estimated units go into the table
    net_est_num_bmr = _get_value_for_name(
        table, nvs, 'net_estimated_num_units_bmr', return_multiple=True)
    assert len(net_est_num_bmr) == 0

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('number_of_units', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '14', d)]),
        Entry('3',
              TCO.NAME,
              [NameValue('num_units', '2', d)]),
        Entry('4',
              TCO.NAME,
              [NameValue('num_units', '4', d)]),
    ]
    proj_tco = Project('uuid1', entries2, basic_graph)
    nvs = table.rows(proj_tco)
    net_num_units = _get_value_for_name(table, nvs, 'net_num_units',
                                        return_multiple=True)
    assert len(net_num_units) == 3
    assert net_num_units[0] == '6'
    assert net_num_units[1] == '7'
    assert net_num_units[2] == '10'


CompletedUnitRow = \
    namedtuple('CompletedUnitRow',
               ['num_units_completed', 'date_completed', 'data'])


def _get_values_for_completed_units(table, rows):
    result = []
    for row in rows:
        num_units_completed = row[table.index(table.NUM_UNITS_COMPLETED)]
        date_completed = row[table.index(table.DATE_COMPLETED)]
        data = row[table.index(table.DATA_SOURCE)]
        result.append(
            CompletedUnitRow(num_units_completed, date_completed, data))
    return result


def test_table_project_units_completed_count(basic_graph, d):
    table = ProjectCompletedUnitCounts()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('current_status', 'complete', d),
               NameValue('completed_date', '2/5/2018', d),
               NameValue('existing_units', '1', d),
               NameValue('proposed_units', '5', d)]),
        Entry('2',  # Ignore PTS since TCO exists
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('current_status', 'complete', d),
               NameValue('completed_date', '2/5/2018', d),
               NameValue('existing_units', '1', d),
               NameValue('proposed_units', '5', d)]),
        Entry('3',
              TCO.NAME,
              [NameValue('num_units', '3', d),
               NameValue('date_issued', '2018/01/01', d)]),
        Entry('4',
              TCO.NAME,
              [NameValue('num_units', '2', d),
               NameValue('date_issued', '2018/01/10', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '6', d),
               NameValue('proposed_units', '5', d)]),
    ]
    proj_tco = Project('uuid1', entries1, basic_graph)
    fields = table.rows(proj_tco)
    num_rows = _get_values_for_completed_units(table, fields)

    # Use TCO data since there are valid records
    assert len(num_rows) == 2
    assert num_rows[0].num_units_completed == '3'
    assert num_rows[0].date_completed == '2018-01-01'
    assert num_rows[0].data == 'tco'
    assert num_rows[1].num_units_completed == '2'
    assert num_rows[1].date_completed == '2018-01-10'
    assert num_rows[1].data == 'tco'

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'abc', d),
               NameValue('current_status', 'complete', d),
               NameValue('completed_date', '2/5/2018', d),
               NameValue('existing_units', '1', d),
               NameValue('proposed_units', '5', d)]),
        Entry('2',  # De-dupe permit numbers
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'abc', d),
               NameValue('current_status', 'complete', d),
               NameValue('completed_date', '2/5/2018', d),
               NameValue('existing_units', '1', d),
               NameValue('proposed_units', '5', d)]),
        Entry('3',
              PTS.NAME,
              [NameValue('permit_type', '2', d),
               NameValue('permit_number', 'xyz', d),
               NameValue('current_status', 'complete', d),
               NameValue('completed_date', '2/10/2018', d),
               NameValue('proposed_units', '2', d)]),
        Entry('4',  # Ignore permits that are not complete
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'def', d),
               NameValue('current_status', 'filed', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '5', d)]),
    ]
    proj_pts = Project('uuid1', entries2, basic_graph)
    fields = table.rows(proj_pts)
    num_rows = _get_values_for_completed_units(table, fields)

    # Use PTS data since there are valid records
    assert len(num_rows) == 2
    assert num_rows[0].num_units_completed == '4'
    assert num_rows[0].date_completed == '2018-02-05'
    assert num_rows[0].data == 'dbi'
    assert num_rows[1].num_units_completed == '2'
    assert num_rows[1].date_completed == '2018-02-10'
    assert num_rows[1].data == 'dbi'


@pytest.fixture
def unit_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    return rg


def test_project_details_unique(basic_graph, d):
    table = ProjectDetails()

    entries = [
        Entry('1',
              Planning.NAME,
              [NameValue('residential_units_1br_net', '10', d)]),
        Entry('2',
              MOHCDPipeline.NAME,
              [NameValue('project_id', 'mohcd1', d),
               NameValue('num_1bd_units', '9', d)]),
    ]
    proj = Project('uuid1', entries, basic_graph)
    nvs = table.rows(proj)

    assert _get_value_for_name(table,
                               nvs,
                               table.OUT_1BR,
                               return_multiple=True) == ['9']


def test_project_details_bedroom_info(unit_graph, d):
    table = ProjectDetails()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('residential_units_1br_exist', '0', d),
               NameValue('residential_units_1br_prop', '10', d)]),
    ]
    proj_normal = Project('uuid1', entries1, unit_graph)

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('residential_units_adu_1br_exist', '0', d),
               NameValue('residential_units_adu_1br_prop', '1', d)]),
    ]
    proj_adu = Project('uuid2', entries2, unit_graph)

    nvs = table.rows(proj_normal)
    assert _get_value_for_name(table, nvs, 'residential_units_1br') == '10'

    nvs = table.rows(proj_adu)
    assert _get_value_for_name(table, nvs, 'residential_units_adu_1br') == '1'
    assert _get_value_for_name(table, nvs, 'is_adu') == 'TRUE'


def test_project_details_permit_addenda_summary(basic_graph, d):
    table = ProjectDetails()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('residential_units_1br_exist', '0', d),
               NameValue('residential_units_1br_prop', '2', d)]),
        Entry('2',
              PermitAddendaSummary.NAME,
              [NameValue('permit_number', 'xyz', d),
               NameValue('earliest_addenda_arrival', '2015-01-01', d)]),
        # Use the earliest addenda arrival date
        Entry('3',
              PermitAddendaSummary.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('earliest_addenda_arrival', '2015-02-02', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj_normal)
    assert _get_value_for_name(table, nvs, 'earliest_addenda_arrival') == \
        '2015-01-01'


def test_project_details_env_review_type(basic_graph, d):
    table = ProjectDetails()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('residential_units_1br_exist', '0', d),
               NameValue('residential_units_1br_prop', '2', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('environmental_review_type',
                         'Categorical Exemption-Certificate', d),
               NameValue('residential_units_1br_prop', '2', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj_normal)
    assert _get_value_for_name(table, nvs, 'environmental_review_type') == \
        'Categorical Exemption-Certificate'
    assert _get_value_for_name(table,
                               nvs,
                               'environmental_review_type_bucketed') == \
        'Categorical Exemption'


def test_project_details_is_da(basic_graph, d):
    table = ProjectDetails()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('record_id', 'abc', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('record_id', 'xyz', d)])
    ]
    # No PHA records in Planning or OEWD entries
    proj = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj)

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('record_id', 'abc', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('record_id', 'xyz', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'PHA', d),
               NameValue('record_id', 'def', d)]),
    ]
    # Existence of PHA records means it's a DA
    proj = Project('uuid1', entries2, basic_graph)
    nvs = table.rows(proj)
    assert _get_value_for_name(table, nvs, 'is_da') == 'TRUE'

    entries3 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('record_id', 'abc', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d)]),
        Entry('3',
              OEWDPermits.NAME,
              [NameValue('row_number', '1', d),
               NameValue('delivery_agency', 'OCII', d)]),
    ]
    # Existence of OEWD permits node means it's a DA
    proj = Project('uuid1', entries3, basic_graph)
    nvs = table.rows(proj)
    assert _get_value_for_name(table, nvs, 'is_da') == 'TRUE'


def test_project_details_is_rehab(basic_graph, d):
    table = ProjectDetails()

    tests = [
        EntriesTestRow(
            name='rehab project',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_net', '2', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', 'mohcd1', d),
                       NameValue('project_type', 'Rehabilitation', d)]),
            ],
            want={
                'is_rehab': 'TRUE',
            }),
        EntriesTestRow(
            name='non-rehab project',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_net', '2', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', 'mohcd1', d),
                       NameValue('project_type', 'New Construction', d)]),
            ],
            want={
                'is_rehab': 'FALSE',
            }),
    ]
    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_name(table,
                                       fields,
                                       name) == wantvalue, test.name


def test_project_details_incentives(basic_graph, d):
    table = ProjectDetails()

    tests = [
        EntriesTestRow(
            name='sb330 and ab2162',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('sb330', 'CHECKED', d),
                       NameValue('ab2162', 'CHECKED', d),
                       NameValue('sb35', '', d)]),
            ],
            want={
                'sb330': 'TRUE',
                'ab2162': 'TRUE',
            }),
        EntriesTestRow(
            name='housing_sustainability_district and state density bonus',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('sb330', '', d),
                       NameValue('housing_sustainability_dist', 'CHECKED', d),
                       NameValue('state_density_bonus_individual',
                                 'CHECKED', d)]),
            ],
            want={
                'housing_sustainability_dist': 'TRUE',
                'state_density_bonus': 'TRUE',
            }),
    ]
    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_name(table,
                                       fields,
                                       name) == wantvalue, test.name


def test_project_details_bedroom_info_mohcd(basic_graph, d):
    table = ProjectDetails()

    tests = [
        EntriesTestRow(
            name='only pull information from pipeline even if inclusionary',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_net', '2', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', 'mohcd1', d),
                       NameValue('num_1bd_units', '10', d)]),
                Entry('3',
                      MOHCDInclusionary.NAME,
                      [NameValue('project_id', 'mohcd1', d),
                       NameValue('num_2bd_units', '5', d),
                       NameValue('num_3bd_units', '3', d)]),
            ],
            want={
                'residential_units_1br': '10',
                'residential_units_2br': '',
                'residential_units_3br': '',
            }),
        EntriesTestRow(
            name='pull info from inclusionary because no other choice',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      MOHCDInclusionary.NAME,
                      [NameValue('project_id', 'mohcd1', d),
                       NameValue('num_2bd_units', '5', d),
                       NameValue('num_3bd_units', '3', d)]),
            ],
            want={
                'residential_units_1br': '',  # ignore planning data
                'residential_units_2br': '5',
                'residential_units_3br': '3',
            }),
        EntriesTestRow(
            name='at least one non-zero bedroom count to emit',
            entries=[
                Entry('1',
                      Planning.NAME,
                      []),
                Entry('2',
                      MOHCDInclusionary.NAME,
                      [NameValue('project_id', 'mohcd1', d),
                       NameValue('num_2bd_units', '0', d)]),
            ],
            want={
                'residential_units_2br': '',
            }),
    ]
    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_name(table,
                                       fields,
                                       name) == wantvalue, test.name


def test_project_details_ami_info_mohcd(basic_graph, d):
    table = ProjectDetails()

    tests = [
        EntriesTestRow(
            name='only pull information from pipeline even if inclusionary',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_net', '2', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('num_20_percent_ami_units', '10', d)]),
                Entry('3',
                      MOHCDInclusionary.NAME,
                      [NameValue('num_20_percent_ami_units', '5', d),
                       NameValue(
                           'num_more_than_120_percent_ami_units',
                           '3',
                           d)]),
            ],
            want={
                'num_20_percent_ami_units': '10',
                'num_more_than_120_percent_ami_units': '',
            }),
        EntriesTestRow(
            name='pull info from inclusionary because no other choice',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      MOHCDInclusionary.NAME,
                      [NameValue('num_20_percent_ami_units', '5', d),
                       NameValue(
                           'num_more_than_120_percent_ami_units',
                           '3',
                           d)]),
            ],
            want={
                'residential_units_1br': '2',
                'num_20_percent_ami_units': '5',
                'num_more_than_120_percent_ami_units': '3',
            }),
    ]
    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_name(table,
                                       fields,
                                       name) == wantvalue, test.name


def test_project_details_is_100pct_affordable_mohcd(basic_graph, d):
    table = ProjectDetails()

    tests = [
        EntriesTestRow(
            name='true case',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('total_project_units', '10', d),
                       NameValue('total_affordable_units', '10', d)]),
            ],
            want='TRUE'),
        EntriesTestRow(
            name='false case',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('total_project_units', '4', d),
                       NameValue('total_affordable_units', '3', d)]),
            ],
            want='FALSE'),
        EntriesTestRow(
            name='pull from affordable rental portfolio',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      AffordableRentalPortfolio.NAME,
                      [NameValue('total_project_units', '4', d),
                       NameValue('total_affordable_units', '4', d)]),
            ],
            want='TRUE'),
        EntriesTestRow(
            name='ignore zero-valued projects',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      AffordableRentalPortfolio.NAME,
                      [NameValue('total_project_units', '0', d),
                       NameValue('total_affordable_units', '0', d)]),
            ],
            want=''),
        EntriesTestRow(
            name='is close enough',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('total_project_units', '10', d),
                       NameValue('total_affordable_units', '9', d)]),
            ],
            want='TRUE'),
        EntriesTestRow(
            name='pull from oewd and is close enough',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '0', d),
                       NameValue('residential_units_1br_prop', '2', d)]),
                Entry('2',
                      OEWDPermits.NAME,
                      [NameValue('total_units', '10', d),
                       NameValue('affordable_units', '9', d),
                       NameValue('delivery_agency', 'OCII', d)]),
            ],
            want='TRUE'),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        nvs = table.rows(proj)

        assert _get_value_for_name(table,
                                   nvs,
                                   'is_100pct_affordable') == test.want, \
            'Failed "%s"' % test.name


def test_project_details_section_415(basic_graph, d):
    table = ProjectDetails()

    tests = [
        EntriesTestRow(
            name='simple case',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '2', d),
                       NameValue('residential_units_1br_prop', '4', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('section_415_declaration',
                                 'Exempt project',
                                 d)]),
            ],
            want='Exempt project'),
        EntriesTestRow(
            name='unset case',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('residential_units_1br_exist', '2', d),
                       NameValue('residential_units_1br_prop', '4', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('total_project_units', '4', d),
                       NameValue('total_affordable_units', '3', d)]),
            ],
            want=''),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        nvs = table.rows(proj)

        assert _get_value_for_name(
                table,
                nvs,
                'inclusionary_housing_program_status') == test.want, \
            'Failed "%s"' % test.name


StatusRow = namedtuple('StatusRow',
                       ['top_level_status', 'start_date', 'end_date'])


def _get_values_for_status(table, rows):
    result = []
    for row in rows:
        top_level_status = row[table.index(table.TOP_LEVEL_STATUS)]
        start_date = row[table.index(table.START_DATE)]
        end_date = row[table.index(table.END_DATE)]
        result.append(StatusRow(top_level_status, start_date, end_date))
    return result


@pytest.fixture
def child_parent_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    rg.add(Node(record_id='2', parents=['1']))
    rg.add(Node(record_id='3', parents=['1']))
    rg.add(Node(record_id='4', parents=['1']))
    rg.add(Node(record_id='5', parents=['1']))
    rg.add(Node(record_id='6', parents=['1']))
    rg.add(Node(record_id='7', parents=['1']))
    rg.add(Node(record_id='8', parents=['1']))
    rg.add(Node(record_id='9'))
    return rg


def test_project_status_history_under_ent_review(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('date_application_submitted', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'open', d),
               NameValue('date_opened', '2000-01-02', d)]),
    ]

    proj = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Use the application submitted date if it exists
    assert len(status_rows) == 1
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-18'
    assert status_rows[0].end_date == ''

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('date_application_submitted', '2018-11-18', d),
               NameValue('date_application_accepted', '2018-10-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'open', d),
               NameValue('date_opened', '2000-01-02', d)]),
    ]

    proj = Project('uuid1', entries2, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Use the application accepted date if earlier than submitted date
    assert len(status_rows) == 1
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-10-18'
    assert status_rows[0].end_date == ''

    entries3 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('date_opened', '2018-11-18 00:00:00', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('date_opened', '2018-11-25', d)]),
        # Ignore any ENT records that don't have oldest open date
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'ENV', d),
               NameValue('date_opened', '2018-11-28', d)]),
        # Ignore any non-ENT records
        Entry('4',
              Planning.NAME,
              [NameValue('record_type', 'ABC', d),
               NameValue('date_opened', '2018-11-30', d)]),
        # Ignore any invalid status keywords
        Entry('5',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'closed-cancelled', d),
               NameValue('date_opened', '2000-01-02', d)]),
        # Ignore any record not part of the PRJ
        Entry('9',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('date_opened', '2018-11-01', d)]),
    ]

    proj = Project('uuid1', entries3, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Filed status from earliest open ENT child record.
    assert len(status_rows) == 1
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == ''


def test_project_status_history_entitled(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('date_application_submitted', '2018-11-25', d),
               NameValue('date_entitlements_approved',
                         '2019-04-15 00:00:00', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'open', d),
               NameValue('date_opened', '2000-01-02', d)]),
    ]

    proj = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Use the explicit entitlements approved date if provided
    assert len(status_rows) == 2
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == ''

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        # Ignore any ENT records that don't have newest closed date
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        # Ignore any non-ENT records
        Entry('4',
              Planning.NAME,
              [NameValue('record_type', 'ABC', d),
               NameValue('status', 'closed', d),
               NameValue('date_opened', '2018-11-30', d),
               NameValue('date_closed', '2019-05-19', d)]),
        # Ignore any record not part of the PRJ
        Entry('9',
              Planning.NAME,
              [NameValue('record_type', 'ENV', d),
               NameValue('status', 'Closed', d),
               NameValue('date_opened', '2018-11-01', d),
               NameValue('date_closed', '2019-05-28', d)]),
    ]

    proj = Project('uuid1', entries2, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Status looking at closed
    assert len(status_rows) == 2
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == ''

    entries3 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        # ENT is still open, project is not entitlded
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-25', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              Planning.NAME,
              [NameValue('record_type', 'PPA', d),
               NameValue('date_opened', '2018-10-18', d),
               NameValue('date_closed', '2019-01-01', d)]),
    ]

    proj_not_entitled = Project('uuid1', entries3, child_parent_graph)
    fields = table.rows(proj_not_entitled)
    status_rows = _get_values_for_status(table, fields)
    assert len(status_rows) == 1
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == ''

    entries4 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Closed', d),
               NameValue('date_opened', '2018-11-18', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d)]),
    ]

    proj_prj_closed = Project('uuid1', entries4, child_parent_graph)
    fields = table.rows(proj_prj_closed)
    status_rows = _get_values_for_status(table, fields)
    assert len(status_rows) == 2
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-28'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == ''


def test_project_status_history_filed_for_permits(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-03', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PTS.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('permit_type', '1', d),
               NameValue('filed_date', '06/02/2019', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_number', 'xyz', d),
               NameValue('permit_type', '1', d),
               NameValue('filed_date', '06/10/2019', d)]),
    ]

    proj = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Use the earliest filed date for filed for permits status
    assert len(status_rows) == 3
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == '2019-06-02'
    assert status_rows[2].top_level_status == 'filed_for_permits'
    assert status_rows[2].start_date == '2019-06-02'
    assert status_rows[2].end_date == ''

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PTS.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('permit_type', '1', d),
               NameValue('filed_date', '01/10/2019', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_number', 'xyz', d),
               NameValue('permit_type', '1', d),
               NameValue('filed_date', '06/10/2019', d)]),
    ]

    proj = Project('uuid1', entries2, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # If earliest filed date is before the entitled date, make the
    # entitled date equivalent to filed date (no maintain sequentiality)
    assert len(status_rows) == 3
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == '2019-04-15'
    assert status_rows[2].top_level_status == 'filed_for_permits'
    assert status_rows[2].start_date == '2019-04-15'
    assert status_rows[2].end_date == ''


def test_project_status_history_under_construction(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PTS.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('permit_type', '1', d),
               NameValue('site_permit', 'Y', d),
               NameValue('filed_date', '06/02/2019', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'xyz', d),
               NameValue('current_status', 'issued', d),
               NameValue('proposed_units', '7', d),
               NameValue('site_permit', 'Y', d),
               NameValue('first_construction_document_date',
               '10/10/2019', d)]),
        Entry('6',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'abcd', d),
               NameValue('current_status', 'complete', d),
               NameValue('proposed_units', '7', d),
               NameValue('site_permit', 'Y', d),
               NameValue('first_construction_document_date',
               '11/11/2019', d)]),
    ]

    proj = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Use the earliest first construction document date for under
    # construction status if the permit is a site permit
    assert len(status_rows) == 4
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == '2019-06-02'
    assert status_rows[2].top_level_status == 'filed_for_permits'
    assert status_rows[2].start_date == '2019-06-02'
    assert status_rows[2].end_date == '2019-10-10'
    assert status_rows[3].top_level_status == 'under_construction'
    assert status_rows[3].start_date == '2019-10-10'
    assert status_rows[3].end_date == ''

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PTS.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('permit_type', '1', d),
               NameValue('filed_date', '06/02/2019', d),
               NameValue('issued_date', '07/02/2019', d)]),
    ]

    proj = Project('uuid1', entries2, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)

    # Use the earliest issue date for under construction date if permit
    # is not a site permit
    assert len(status_rows) == 4
    assert status_rows[3].top_level_status == 'under_construction'
    assert status_rows[3].start_date == '2019-07-02'
    assert status_rows[3].end_date == ''


def test_project_status_history_completed_construction(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PTS.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('permit_type', '1', d),
               NameValue('site_permit', 'Y', d),
               NameValue('filed_date', '06/02/2019', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'xyz', d),
               NameValue('current_status', 'complete', d),
               NameValue('proposed_units', '7', d),
               NameValue('site_permit', 'Y', d),
               NameValue('first_construction_document_date',
               '11/11/2019', d)]),
        Entry('6',
              TCO.NAME,
              [NameValue('building_permit_type', 'CFC', d),
               NameValue('num_units', '7', d),
               NameValue('date_issued', '2020/01/15', d)]),
    ]

    proj = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # If a project has been CFC'ed then it is complete
    assert len(status_rows) == 5
    assert status_rows[0].top_level_status == 'under_entitlement_review'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == '2019-06-02'
    assert status_rows[2].top_level_status == 'filed_for_permits'
    assert status_rows[2].start_date == '2019-06-02'
    assert status_rows[2].end_date == '2019-11-11'
    assert status_rows[3].top_level_status == 'under_construction'
    assert status_rows[3].start_date == '2019-11-11'
    assert status_rows[3].end_date == '2020-01-15'
    assert status_rows[4].top_level_status == 'completed_construction'
    assert status_rows[4].start_date == '2020-01-15'
    assert status_rows[4].end_date == ''

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PTS.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('permit_type', '1', d),
               NameValue('site_permit', 'Y', d),
               NameValue('filed_date', '02/06/2019', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'xyz', d),
               NameValue('current_status', 'complete', d),
               NameValue('proposed_units', '7', d),
               NameValue('site_permit', 'Y', d),
               NameValue('first_construction_document_date',
               '11/11/2019', d)]),
        Entry('6',
              TCO.NAME,
              [NameValue('building_permit_type', 'TCO', d),
               NameValue('num_units', '4', d),
               NameValue('date_issued', '2020/01/15', d)]),
        Entry('7',
              TCO.NAME,
              [NameValue('building_permit_type', 'TCO', d),
               NameValue('num_units', '3', d),
               NameValue('date_issued', '2020/01/30', d)]),
    ]

    proj = Project('uuid1', entries2, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Add up all the TCO'ed units and check if that is equivalent
    # to all of the project units
    assert len(status_rows) == 5
    assert status_rows[4].top_level_status == 'completed_construction'
    assert status_rows[4].start_date == '2020-01-30'
    assert status_rows[4].end_date == ''

    entries3 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PermitAddendaSummary.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('earliest_addenda_arrival', '2019-06-02', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'abc', d),
               NameValue('current_status', 'complete', d),
               NameValue('proposed_units', '7', d),
               NameValue('filed_date', '06/02/2019', d),
               NameValue('completed_date', '2/1/2020', d),
               NameValue('site_permit', 'Y', d),
               NameValue('first_construction_document_date',
               '11/11/2019', d)]),
        Entry('6',
              TCO.NAME,
              [NameValue('building_permit_type', 'TCO', d),
               NameValue('num_units', '4', d),
               NameValue('date_issued', '2020/01/15', d)]),
        Entry('7',
              TCO.NAME,
              [NameValue('building_permit_type', 'TCO', d),
               NameValue('num_units', '2', d),
               NameValue('date_issued', '2020/01/30', d)]),
        Entry('8',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'xyz', d),
               NameValue('current_status', 'complete', d),
               NameValue('proposed_units', '7', d),
               NameValue('site_permit', 'Y', d),
               NameValue('completed_date', '2/10/2020', d)]),
    ]

    proj = Project('uuid1', entries3, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # If the permits in PTS are all closed use the latest completed date
    assert len(status_rows) == 5
    assert status_rows[4].top_level_status == 'completed_construction'
    assert status_rows[4].start_date == '2020-02-10'
    assert status_rows[4].end_date == ''

    entries4 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '2018-11-18', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '2018-11-25', d),
               NameValue('date_closed', '2019-03-27', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '2018-11-28', d),
               NameValue('date_closed', '2019-04-15', d)]),
        Entry('4',
              PTS.NAME,
              [NameValue('permit_number', 'abc', d),
               NameValue('permit_type', '1', d),
               NameValue('site_permit', 'Y', d),
               NameValue('filed_date', '06/02/2019', d)]),
        Entry('5',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'abc', d),
               NameValue('current_status', 'complete', d),
               NameValue('proposed_units', '7', d),
               NameValue('completed_date', '2/1/2020', d),
               NameValue('site_permit', 'Y', d),
               NameValue('first_construction_document_date',
               '11/11/2019', d)]),
        Entry('6',
              TCO.NAME,
              [NameValue('building_permit_type', 'TCO', d),
               NameValue('num_units', '4', d),
               NameValue('date_issued', '2020/01/15', d)]),
        Entry('8',
              PTS.NAME,
              [NameValue('permit_type', '2', d),
               NameValue('permit_number', 'xyz', d),
               NameValue('current_status', 'issued', d),
               NameValue('site_permit', 'Y', d),
               NameValue('proposed_units', '2', d)]),
        Entry('8',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('permit_number', 'abcd', d),
               NameValue('current_status', 'complete', d),
               NameValue('proposed_units', '2', d),
               NameValue('site_permit', 'Y', d),
               NameValue('completed_date', '2/10/2020', d)]),
    ]

    proj = Project('uuid1', entries4, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # If a permit in PTS is still open then it is not completed
    assert len(status_rows) == 4
