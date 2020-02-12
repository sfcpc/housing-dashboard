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
from schemaless.sources import PPTS
from schemaless.sources import PTS


def test_table_project_facts_atleast_one_measure():
    table = ProjectFacts()

    RowTest = namedtuple('RowTest', ['input', 'want'])
    tests = [
        RowTest(['', ''], False),  # empty row
        RowTest(['', '0'], True),  # zero different from empty
        RowTest(['1', '2'], True),  # normal full row
    ]
    for test in tests:
        row = [''] * len(table.header())
        row[table.index(table.NET_NUM_UNITS)] = test.input[0]
        row[table.index(table.NET_NUM_UNITS_BMR)] = test.input[1]
        assert table._atleast_one_measure(row) == test.want


def _get_value_for_name(table, rows, name, return_multiple=False):
    result = []
    if len(rows) == 1:
        return rows[0][table.index(name)]
    elif len(rows) > 1:
        for row in rows:
            row_name = row[table.index(table.NAME)]
            if row_name == name:
                row_value = row[table.index(table.VALUE)]
                if return_multiple:
                    result.append(row_value)
                else:
                    return row_value
        return '' if not return_multiple else sorted(result)


@pytest.fixture
def basic_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    rg.add(Node(record_id='2', parents=['1']))
    return rg


def test_table_project_facts_units(basic_graph):
    d = datetime.fromisoformat('2019-01-01')
    table = ProjectFacts()

    entries1 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '5', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    fields = table.rows(proj_normal)
    # Gets from PTS because it's present
    assert _get_value_for_name(table, fields, 'net_num_units') == '-2'

    entries2 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2', PTS.NAME, [NameValue('proposed_units', '7', d)]),
    ]
    proj_no_permit_type = Project('uuid1', entries2, basic_graph)
    fields = table.rows(proj_no_permit_type)
    # Gets from PPTS because PTS data is incomplete (no permit type)
    assert _get_value_for_name(table, fields, 'net_num_units') == '10'

    entries3 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2', PTS.NAME, [NameValue('permit_type', '1', d),
                              NameValue('existing_units', '7', d)]),
    ]
    proj_missing_proposed_units = Project('uuid1', entries3, basic_graph)
    fields = table.rows(proj_missing_proposed_units)
    # Gets from PPTS because PTS data is incomplete (proper permit type, no
    # proposed)
    assert _get_value_for_name(table, fields, 'net_num_units') == '10'

    entries4 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('proposed_units', '7', d)]),
    ]
    proj_missing_existing = Project('uuid1', entries4, basic_graph)
    fields = table.rows(proj_missing_existing)
    # Gets from PTS because we can infer with just proposed
    assert _get_value_for_name(table, fields, 'net_num_units') == '7'

    entries5 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
    ]
    proj_ppts_only = Project('uuid1', entries5, basic_graph)
    fields = table.rows(proj_ppts_only)
    # Gets from PPTS because no other choice
    assert _get_value_for_name(table, fields, 'net_num_units') == '10'


def test_table_project_units_full_count(basic_graph):
    d = datetime.fromisoformat('2019-01-01')
    table = ProjectUnitCountsFull()

    entries1 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '5', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = table.rows(proj_normal)
    net_num_units = _get_value_for_name(table, nvs, 'net_num_units',
                                        return_multiple=True)
    assert len(net_num_units) == 2
    assert net_num_units[0] == '-2'
    assert net_num_units[1] == '10'


@pytest.fixture
def unit_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    return rg


def test_project_details_bedroom_info(unit_graph):
    d = datetime.fromisoformat('2019-01-01')
    table = ProjectDetails()

    entries1 = [
        Entry('1',
              PPTS.NAME,
              [NameValue('residential_units_1br_net', '10', d)]),
    ]
    proj_normal = Project('uuid1', entries1, unit_graph)

    entries2 = [
        Entry('1',
              PPTS.NAME,
              [NameValue('residential_units_adu_1br_net', '1', d)]),
    ]
    proj_adu = Project('uuid2', entries2, unit_graph)

    nvs = table.rows(proj_normal)
    assert _get_value_for_name(table, nvs, 'residential_units_1br') == '10'

    nvs = table.rows(proj_adu)
    assert _get_value_for_name(table, nvs, 'residential_units_adu_1br') == '1'
    assert _get_value_for_name(table, nvs, 'is_adu') == 'TRUE'


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


def test_project_status_history_predevelopment(child_parent_graph):
    d = datetime.fromisoformat('2019-01-01')
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              PPTS.NAME,
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
              PPTS.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        Entry('2',
              PPTS.NAME,
              [NameValue('record_type_category', 'PPA', d),
               NameValue('date_opened', '10/18/2018 12:00:00 AM +0000', d)]),
        # ENT's don't matter for pre-development open date but do for end_date
        Entry('3',
              PPTS.NAME,
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


def test_project_status_history_filed_for_entitlements(child_parent_graph):
    d = datetime.fromisoformat('2019-01-01')
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              PPTS.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        Entry('2',
              PPTS.NAME,
              [NameValue('record_type_category', 'CUA', d),
               NameValue('date_opened', '11/25/2018 12:00:00 AM +0000', d)]),
        # Ignore any ENT records that don't have oldest open date
        Entry('3',
              PPTS.NAME,
              [NameValue('record_type_category', 'ENV', d),
               NameValue('date_opened', '11/28/2018 12:00:00 AM +0000', d)]),
        # Ignore any non-ENT records
        Entry('4',
              PPTS.NAME,
              [NameValue('record_type_category', 'ABC', d),
               NameValue('date_opened', '11/30/2018 12:00:00 AM +0000', d)]),
        # Ignore any record not part of the PRJ
        Entry('5',
              PPTS.NAME,
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


def test_project_status_history_entitled(child_parent_graph):
    d = datetime.fromisoformat('2019-01-01')
    table = ProjectStatusHistory()

    entries1 = [
        Entry('1',
              PPTS.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        # Ignore any ENT records that don't have newest closed date
        Entry('2',
              PPTS.NAME,
              [NameValue('record_type_category', 'CUA', d),
               NameValue('status', 'Closed - CEQA', d),
               NameValue('date_opened', '11/25/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '3/27/2019 12:00:00 AM +0000', d)]),
        Entry('3',
              PPTS.NAME,
              [NameValue('record_type_category', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '11/28/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '4/15/2019 12:00:00 AM +0000', d)]),
        # Ignore any non-ENT records
        Entry('4',
              PPTS.NAME,
              [NameValue('record_type_category', 'ABC', d),
               NameValue('status', 'closed', d),
               NameValue('date_opened', '11/30/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '5/20/2019 12:00:00 AM +0000', d)]),
        # Ignore any record not part of the PRJ
        Entry('5',
              PPTS.NAME,
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
              PPTS.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d)]),
        # ENT is still open, project is not entitlded
        Entry('2',
              PPTS.NAME,
              [NameValue('record_type_category', 'CUA', d),
               NameValue('status', 'Accepted', d),
               NameValue('date_opened', '11/25/2018 12:00:00 AM +0000', d)]),
        Entry('3',
              PPTS.NAME,
              [NameValue('record_type_category', 'VAR', d),
               NameValue('status', 'Closed - XYZ', d),
               NameValue('date_opened', '11/28/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '4/15/2019 12:00:00 AM +0000', d)]),
        Entry('4',
              PPTS.NAME,
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
              PPTS.NAME,
              [NameValue('record_type_category', 'PRJ', d),
               NameValue('status', 'Closed', d),
               NameValue('date_opened', '11/18/2018 12:00:00 AM +0000', d),
               NameValue('date_closed', '4/15/2019 12:00:00 AM +0000', d)]),
        Entry('2',
              PPTS.NAME,
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
