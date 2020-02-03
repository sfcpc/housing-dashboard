# Lint as: python3
from datetime import datetime
from collections import namedtuple

import pytest

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from relational.generators import atleast_one_measure
from relational.generators import nv_bedroom_info
from schemaless.create_uuid_map import Node
from schemaless.create_uuid_map import RecordGraph


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


def test_nv_bedroom_info(unit_graph):
    def _get_name_value(nvs, name):
        for nv in nvs:
            if name == nv.name:
                return nv.value
        return ''

    d = datetime.fromisoformat('2019-01-01')

    entries1 = [
        Entry('1', 'ppts', [NameValue('residential_units_1br_net', '10', d)]),
    ]
    proj_normal = Project('uuid1', entries1, unit_graph)

    entries2 = [
        Entry('1', 'ppts', [NameValue('residential_units_adu_1br_net',
                                      '1', d)]),
    ]
    proj_adu = Project('uuid2', entries2, unit_graph)

    nvs = nv_bedroom_info(proj_normal)
    assert _get_name_value(nvs, 'residential_units_1br') == '10'

    nvs = nv_bedroom_info(proj_adu)
    assert _get_name_value(nvs, 'residential_units_adu_1br') == '1'
    assert _get_name_value(nvs, 'is_adu') == 'TRUE'
