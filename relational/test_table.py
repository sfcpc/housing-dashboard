# Lint as: python3
from datetime import datetime
from collections import namedtuple

import pytest

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.table import ProjectDetails
from relational.table import ProjectFacts
from relational.table import ProjectStatusHistory
from relational.table import ProjectUnitCountsFull
from schemaless.create_uuid_map import Node
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
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
    return rg


EntriesTestRow = namedtuple('EntriesTestRow', ['name', 'entries', 'want'])


def test_table_project_facts_atleast_one_measure():
    table = ProjectFacts()

    RowTest = namedtuple('RowTest', ['input', 'want'])
    tests = [
        RowTest(['', '', ''], False),  # empty row
        RowTest(['', '0', ''], True),  # zero different from empty
        RowTest(['1', '2', ''], True),  # normal full row
        RowTest(['', '', '1'], True),  # estimated field
    ]
    for test in tests:
        row = [''] * len(table.header())
        row[table.index(table.NET_NUM_UNITS)] = test.input[0]
        row[table.index(table.NET_NUM_UNITS_BMR)] = test.input[1]
        row[table.index(table.NET_EST_NUM_UNITS_BMR)] = test.input[2]
        assert table._atleast_one_measure(row) == test.want


def test_table_project_facts(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='use name if no address',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_market_rate_units', '10', d)]),
            ],
            want={'address': 'BALBOA RESERVOIR DEVELOPMENT'}),
        EntriesTestRow(
            name='always use mohcd if information found',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_market_rate_units', '10', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', '1', d),
                       NameValue('street_number', '123', d),
                       NameValue('street_name', 'chris', d),
                       NameValue('street_type', 'st', d),
                       NameValue('zip_code', '94123', d)]),
            ],
            want={'address': '123 chris st, 94123'}),
        EntriesTestRow(
            name='incorporate mohcd name if found',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('name', 'BALBOA RESERVOIR DEVELOPMENT', d),
                       NameValue('number_of_market_rate_units', '10', d)]),
                Entry('2',
                      MOHCDPipeline.NAME,
                      [NameValue('project_id', '1', d),
                       NameValue('project_name', 'chris place', d),
                       NameValue('street_number', '123', d),
                       NameValue('street_name', 'chris', d),
                       NameValue('street_type', 'st', d),
                       NameValue('zip_code', '94123', d)]),
            ],
            want={'address': 'chris place, 123 chris st, 94123'}),
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        for (name, wantvalue) in test.want.items():
            assert _get_value_for_row(table,
                                      fields,
                                      name) == wantvalue, test.name


def test_table_project_facts_units(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='simple test',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
            ],
            want='10'),
        EntriesTestRow(
            name='get from PTS because permit_type 3 is also valid',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
            name='dont use 0 pts unit count if planning wasnt explicitly set',
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
    ]

    for test in tests:
        proj = Project('uuid1', test.entries, basic_graph)
        fields = table.rows(proj)

        assert _get_value_for_row(table,
                                  fields,
                                  'net_num_units') == test.want, \
            'Failed "%s"' % test.name


def test_table_project_facts_units_planning_bmr(basic_graph, d):
    table = ProjectFacts()

    tests = [
        EntriesTestRow(
            name='simple bmr calculation',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_market_rate_units', '20', d),
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
                      [NameValue('number_of_market_rate_units', '20', d)]),
            ],
            want={'net_num_units_bmr': '',
                  'net_estimated_num_units_bmr': '4'},
        ),
        EntriesTestRow(
            name='estimated bmr calculation for large project',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_market_rate_units', '30', d)]),
            ],
            want={'net_num_units_bmr': '',
                  'net_estimated_num_units_bmr': '9'},
        ),
        EntriesTestRow(
            name='estimated bmr calculation for small project',
            entries=[
                Entry('1',
                      Planning.NAME,
                      [NameValue('number_of_market_rate_units', '5', d)]),
            ],
            want={'net_num_units_bmr': '',
                  'net_estimated_num_units_bmr': '0'},
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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
                      [NameValue('number_of_market_rate_units', '10', d)]),
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


def test_table_project_units_full_count(basic_graph, d):
    table = ProjectUnitCountsFull()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('number_of_market_rate_units', '10', d)]),
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
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj_normal)
    net_num_units = _get_value_for_name(
        table, nvs, 'net_num_units', return_multiple=True)
    assert len(net_num_units) == 4
    assert net_num_units[0] == '-3'
    assert net_num_units[1] == '0'
    assert net_num_units[2] == '7'
    assert net_num_units[3] == '10'

    net_num_bmr = _get_value_for_name(
        table, nvs, 'net_num_units_bmr', return_multiple=True)
    assert len(net_num_bmr) == 2  # only inferrable data is in MOHCD
    assert net_num_bmr[0] == '0'
    assert net_num_bmr[1] == '5'

    # No estimated units go into the table
    net_est_num_bmr = _get_value_for_name(
        table, nvs, 'net_estimated_num_units_bmr', return_multiple=True)
    assert len(net_est_num_bmr) == 0

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('number_of_market_rate_units', '10', d)]),
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
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj_normal)
    assert _get_value_for_name(table, nvs, 'earliest_addenda_arrival') == \
        '2015-01-01'


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
    rg.add(Node(record_id='5'))
    return rg


def test_project_status_history_predevelopment(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
    ]
    proj_no_ppa = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj_no_ppa)
    # No pre-development status if there is no PPA
    status_rows = _get_values_for_status(table, fields)
    assert len(status_rows) == 0

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type_category', 'PPA', d),
               NameValue('date_opened', '10/18/2018 12:00:00 AM +0000', d)]),
        # ENT's don't matter for pre-development open date but do for end_date
        Entry('3',
              Planning.NAME,
              [NameValue('record_type_category', 'CUA', d),
               NameValue('date_opened', '11/25/2018 12:00:00 AM +0000', d)])
    ]

    proj_with_ppa = Project('uuid1', entries2, child_parent_graph)
    fields = table.rows(proj_with_ppa)
    # Pre-development is taken from the PPA record date
    status_rows = _get_values_for_status(table, fields)
    assert len(status_rows) == 2
    assert status_rows[0].top_level_status == 'pre-development'
    assert status_rows[0].start_date == '2018-10-18'
    assert status_rows[0].end_date == '2018-11-25'
    # Filed status is taken from first open ENT record under PRJ
    assert status_rows[1].top_level_status == 'filed_for_entitlements'
    assert status_rows[1].start_date == '2018-11-25'
    assert status_rows[1].end_date == ''


def test_project_status_history_filed_for_entitlements(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type_category', 'CUA', d),
               NameValue('date_opened', '11/25/2018 12:00:00 AM +0000', d)]),
        # Ignore any ENT records that don't have oldest open date
        Entry('3',
              Planning.NAME,
              [NameValue('record_type_category', 'ENV', d),
               NameValue('date_opened', '11/28/2018 12:00:00 AM +0000', d)]),
        # Ignore any non-ENT records
        Entry('4',
              Planning.NAME,
              [NameValue('record_type_category', 'ABC', d),
               NameValue('date_opened', '11/30/2018 12:00:00 AM +0000', d)]),
        # Ignore any record not part of the PRJ
        Entry('5',
              Planning.NAME,
              [NameValue('record_type_category', 'VAR', d),
               NameValue('date_opened', '11/01/2018 12:00:00 AM +0000', d)]),
    ]

    proj = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Filed status from earliest open ENT child record.
    assert len(status_rows) == 1
    assert status_rows[0].top_level_status == 'filed_for_entitlements'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == ''


def test_project_status_history_entitled(child_parent_graph, d):
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        # Ignore any ENT records that don't have newest closed date
        Entry('2',
              Planning.NAME,
              [NameValue('record_type_category', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '11/25/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '3/27/2019 12:00:00 AM +0000', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type_category', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '11/28/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '4/15/2019 12:00:00 AM +0000', d)]),
        # Ignore any non-ENT records
        Entry('4',
              Planning.NAME,
              [NameValue('record_type_category', 'ABC', d),
               NameValue('status', 'closed', d),
               NameValue('date_opened', '11/30/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '5/20/2019 12:00:00 AM +0000', d)]),
        # Ignore any record not part of the PRJ
        Entry('5',
              Planning.NAME,
              [NameValue('record_type_category', 'ENV', d),
               NameValue('status', 'Closed', d),
               NameValue('date_opened', '11/01/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '5/28/2019 12:00:00 AM +0000', d)]),
    ]

    proj = Project('uuid1', entries1, child_parent_graph)
    fields = table.rows(proj)
    status_rows = _get_values_for_status(table, fields)
    # Status looking at closed
    assert len(status_rows) == 2
    assert status_rows[0].top_level_status == 'filed_for_entitlements'
    assert status_rows[0].start_date == '2018-11-25'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == ''

    entries2 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        # ENT is still open, project is not entitlded
        Entry('2',
              Planning.NAME,
              [NameValue('record_type_category', 'CUA', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '11/25/2018 12:00:00 AM +0000', d)]),
        Entry('3',
              Planning.NAME,
              [NameValue('record_type_category', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '11/28/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '4/15/2019 12:00:00 AM +0000', d)]),
        Entry('4',
              Planning.NAME,
              [NameValue('record_type_category', 'PPA', d),
               NameValue('date_opened', '10/18/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '1/1/2019 12:00:00 AM +0000', d)]),
    ]

    proj_not_entitled = Project('uuid1', entries2, child_parent_graph)
    fields = table.rows(proj_not_entitled)
    status_rows = _get_values_for_status(table, fields)
    assert len(status_rows) == 2
    assert status_rows[0].top_level_status == 'pre-development'
    assert status_rows[0].start_date == '2018-10-18'
    assert status_rows[0].end_date == '2018-11-25'
    assert status_rows[1].top_level_status == 'filed_for_entitlements'
    assert status_rows[1].start_date == '2018-11-25'
    assert status_rows[1].end_date == ''

    entries3 = [
        Entry('1',
              Planning.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('status', 'Closed', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '4/15/2019 12:00:00 AM +0000', d)]),
        Entry('2',
              Planning.NAME,
              [NameValue('record_type_category', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '11/28/2018 12:00:00 AM +0000', d)]),
    ]

    proj_prj_closed = Project('uuid1', entries3, child_parent_graph)
    fields = table.rows(proj_prj_closed)
    status_rows = _get_values_for_status(table, fields)
    assert len(status_rows) == 2
    assert status_rows[0].top_level_status == 'filed_for_entitlements'
    assert status_rows[0].start_date == '2018-11-28'
    assert status_rows[0].end_date == '2019-04-15'
    assert status_rows[1].top_level_status == 'entitled'
    assert status_rows[1].start_date == '2019-04-15'
    assert status_rows[1].end_date == ''
