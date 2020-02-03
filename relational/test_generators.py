# Lint as: python3
from datetime import datetime
from collections import namedtuple

import pytest

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.generators import atleast_one_measure
from relational.generators import gen_units
from relational.generators import nv_bedroom_info
from schemaless.create_uuid_map import Node
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import PPTS
from schemaless.sources import PTS


def test_atleast_one_measure():
    header = ['net_num_units', 'net_num_units_bmr', 'net_num_square_feet']

    RowTest = namedtuple('RowTest', ['input', 'want', 'header'],
                         defaults=[header])
    tests = [
        RowTest(['', '', ''], False),  # empty row
        RowTest(['', '0', ''], True),  # zero different from empty
        RowTest(['1', '2', '3'], True),  # normal full row
        # no relevant measures to filter on
        RowTest(['1', '2', '3'], True, ['a', 'b', 'c']),
    ]
    for test in tests:
        assert atleast_one_measure(test.input, test.header) == test.want


@pytest.fixture
def basic_entries():
    old = datetime.fromisoformat('2019-01-01')
    lessold = datetime.fromisoformat('2020-01-01')

    e = []
    e.append(Entry('1', 'ppts', [NameValue('num_units_bmr', '22', old)]))
    e.append(Entry('2',
                   'ppts',
                   [NameValue('num_units_bmr', '32', lessold),
                    NameValue('num_square_feet', '2300', old),
                    NameValue('residential_units_1br', '1', old)]))
    e.append(Entry('3',
                   'ppts',
                   [NameValue('num_square_feet', '2100', lessold)]))
    return e


@pytest.fixture
def unit_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    return rg


@pytest.fixture
def basic_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    rg.add(Node(record_id='2', parents=['1']))
    return rg


def _get_value_for_name(data, name):
    """Works for both Field and OutputNameValue"""
    for datum in data:
        if name == datum.name:
            return datum.value
    return ''


def test_gen_units(basic_graph):
    d = datetime.fromisoformat('2019-01-01')

    entries1 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2', PTS.NAME, [NameValue('existing_units', '7', d),
                              NameValue('proposed_units', '5', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = gen_units(proj_normal)
    # Gets from PTS because it's present
    assert _get_value_for_name(nvs, 'net_num_units') == '-2'

    entries2 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2', PTS.NAME, [NameValue('existing_units', '7', d)]),
    ]
    proj_missing_proposed_units = Project('uuid1', entries2, basic_graph)
    nvs = gen_units(proj_missing_proposed_units)
    # Gets from PPTS because PTS data is incomplete
    assert _get_value_for_name(nvs, 'net_num_units') == '10'

    entries3 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
    ]
    proj_ppts_only = Project('uuid1', entries3, basic_graph)
    nvs = gen_units(proj_ppts_only)
    # Gets from PPTS because no ohter choice
    assert _get_value_for_name(nvs, 'net_num_units') == '10'


def test_nv_bedroom_info(unit_graph):
    d = datetime.fromisoformat('2019-01-01')

    entries1 = [
        Entry('1', PPTS.NAME,
              [NameValue('residential_units_1br_net', '10', d)]),
    ]
    proj_normal = Project('uuid1', entries1, unit_graph)

    entries2 = [
        Entry('1', PPTS.NAME,
              [NameValue('residential_units_adu_1br_net', '1', d)]),
    ]
    proj_adu = Project('uuid2', entries2, unit_graph)

    nvs = nv_bedroom_info(proj_normal)
    assert _get_value_for_name(nvs, 'residential_units_1br') == '10'

    nvs = nv_bedroom_info(proj_adu)
    assert _get_value_for_name(nvs, 'residential_units_adu_1br') == '1'
    assert _get_value_for_name(nvs, 'is_adu') == 'TRUE'
