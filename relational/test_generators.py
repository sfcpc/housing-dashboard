# Lint as: python3
from datetime import datetime
from collections import namedtuple

import pytest

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.generators import atleast_one_measure
from relational.generators import gen_units
from relational.generators import nv_all_units
from relational.generators import nv_bedroom_info
from schemaless.create_uuid_map import Node
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import PPTS
from schemaless.sources import PTS


def test_atleast_one_measure():
    header = ['net_num_units', 'net_num_units_bmr', 'net_num_square_feet']

    RowTest = namedtuple('RowTest',
                         ['input', 'want', 'header'],
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


def _get_value_for_name(data, name, return_multiple=False):
    """Works for both Field and OutputNameValue"""

    result = []
    for datum in data:
        if name == datum.name:
            if return_multiple:
                result.append(datum.value)
            else:
                return datum.value
    return '' if not return_multiple else sorted(result)


def test_gen_units(basic_graph):
    d = datetime.fromisoformat('2019-01-01')

    entries1 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '5', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    fields = gen_units(proj_normal)
    # Gets from PTS because it's present
    assert _get_value_for_name(fields, 'net_num_units') == '-2'

    entries2 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2', PTS.NAME, [NameValue('proposed_units', '7', d)]),
    ]
    proj_no_permit_type = Project('uuid1', entries2, basic_graph)
    fields = gen_units(proj_no_permit_type)
    # Gets from PPTS because PTS data is incomplete (no permit type)
    assert _get_value_for_name(fields, 'net_num_units') == '10'

    entries3 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2', PTS.NAME, [NameValue('permit_type', '1', d),
                              NameValue('existing_units', '7', d)]),
    ]
    proj_missing_proposed_units = Project('uuid1', entries3, basic_graph)
    fields = gen_units(proj_missing_proposed_units)
    # Gets from PPTS because PTS data is incomplete (proper permit type, no
    # proposed)
    assert _get_value_for_name(fields, 'net_num_units') == '10'

    entries4 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('proposed_units', '7', d)]),
    ]
    proj_missing_existing = Project('uuid1', entries4, basic_graph)
    fields = gen_units(proj_missing_existing)
    # Gets from PTS because we can infer with just proposed
    assert _get_value_for_name(fields, 'net_num_units') == '7'

    entries5 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
    ]
    proj_ppts_only = Project('uuid1', entries5, basic_graph)
    fields = gen_units(proj_ppts_only)
    # Gets from PPTS because no other choice
    assert _get_value_for_name(fields, 'net_num_units') == '10'


def test_nv_all_units(basic_graph):
    d = datetime.fromisoformat('2019-01-01')

    entries1 = [
        Entry('1', PPTS.NAME, [NameValue('market_rate_units_net', '10', d)]),
        Entry('2',
              PTS.NAME,
              [NameValue('permit_type', '1', d),
               NameValue('existing_units', '7', d),
               NameValue('proposed_units', '5', d)]),
    ]
    proj_normal = Project('uuid1', entries1, basic_graph)
    nvs = nv_all_units(proj_normal)
    net_num_units = _get_value_for_name(nvs, 'net_num_units',
                                        return_multiple=True)
    assert len(net_num_units) == 2
    assert net_num_units[0] == '-2'
    assert net_num_units[1] == '10'


def test_nv_bedroom_info(unit_graph):
    d = datetime.fromisoformat('2019-01-01')

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

    nvs = nv_bedroom_info(proj_normal)
    assert _get_value_for_name(nvs, 'residential_units_1br') == '10'

    nvs = nv_bedroom_info(proj_adu)
    assert _get_value_for_name(nvs, 'residential_units_adu_1br') == '1'
    assert _get_value_for_name(nvs, 'is_adu') == 'TRUE'
