# Lint as: python3
from datetime import datetime
from collections import namedtuple

import pytest

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.table import ProjectDetails
from relational.table import ProjectFacts
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
