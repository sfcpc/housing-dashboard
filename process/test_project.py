# Lint as: python3
from datetime import datetime

import pytest

from schemaless.create_uuid_map import Node
from schemaless.create_uuid_map import RecordGraph
from process.project import Entry
from process.project import NameValue
from process.project import Project


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
def basic_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='1'))
    rg.add(Node(record_id='2', parents=['1']))
    rg.add(Node(record_id='3', parents=['1']))
    rg.add(Node(record_id='4', parents=['1']))
    return rg


def test_project_simple_case(basic_entries, basic_graph):
    proj = Project('uuid-0001', basic_entries, basic_graph)
    assert len(proj.roots) == 1
    assert len(proj.roots['ppts']) == 1
    assert proj.roots['ppts'][0].fk == '1'

    # only source data from the parent even if present in child
    assert proj.field('num_units_bmr') == '22'

    # pull data from the child since not present on the parent
    assert proj.field('residential_units_1br') == '1'

    # pull data from the latest child
    assert proj.field('num_square_feet') == '2100'


# def test_project_no_main_record():
#    old = datetime.fromisoformat('2019-01-01')
#    lessold = datetime.fromisoformat('2020-01-01')
#
#    data = four_level_dict()
#    data['ppts']['CUA1']['parent']['value'] = 'PRJ'
#    data['ppts']['CUA1']['parent']['last_updated'] = old
#    data['ppts']['CUA1']['num_units_bmr']['value'] = '22'
#    data['ppts']['CUA1']['num_units_bmr']['last_updated'] = old
#
#    data['ppts']['CUA2']['parent']['value'] = 'PRJ'
#    data['ppts']['CUA2']['parent']['last_updated'] = lessold
#    data['ppts']['CUA2']['num_units_bmr']['value'] = '32'
#    data['ppts']['CUA2']['num_units_bmr']['last_updated'] = lessold
#    data['ppts']['CUA2']['num_square_feet']['value'] = '2300'
#    data['ppts']['CUA2']['num_square_feet']['last_updated'] = lessold
#
#    proj = Project('uuid-0001', data)
#
#    # oldest one got upgraded
#    assert proj.main is not None
#    assert proj.main.key == 'CUA1'
#    assert len(proj.children) == 1
#    assert proj.children[0].key == 'CUA2'
#    assert proj.field('num_units_bmr') == '22'
#
#
# def test_project_field():
#    """Test tie-breaking rules for fields"""
#    old = datetime.fromisoformat('2019-01-01')
#    lessold = datetime.fromisoformat('2020-01-01')
#
#    id = 'uuid-0000'
#    data = four_level_dict()
#    data['ppts']['PRJ']['address']['value'] = '123 goog st'
#    data['ppts']['PRJ']['address']['last_updated'] = old
#    data['ppts']['PRJ']['num_units']['value'] = '144'
#    data['ppts']['PRJ']['num_units']['last_updated'] = old
#    data['ppts']['PRJ']['num_units_bmr']['value'] = '22'
#    data['ppts']['PRJ']['num_units_bmr']['last_updated'] = old
#
#    data['ppts']['ENV']['parent']['value'] = 'PRJ'
#    data['ppts']['ENV']['parent']['last_updated'] = old
#    data['ppts']['ENV']['address']['value'] = '123 Gog st'
#    data['ppts']['ENV']['address']['last_updated'] = old
#    data['ppts']['ENV']['num_square_feet']['value'] = '2200'
#    data['ppts']['ENV']['num_square_feet']['last_updated'] = old
#
#    data['ppts']['CUA1']['parent']['value'] = 'PRJ'
#    data['ppts']['CUA1']['parent']['last_updated'] = old
#
#    data['ppts']['CUA2']['parent']['value'] = 'PRJ'
#    data['ppts']['CUA2']['parent']['last_updated'] = lessold
#    data['ppts']['CUA2']['num_units_bmr']['value'] = '32'
#    data['ppts']['CUA2']['num_units_bmr']['last_updated'] = lessold
#    data['ppts']['CUA2']['num_square_feet']['value'] = '2300'
#    data['ppts']['CUA2']['num_square_feet']['last_updated'] = lessold
#
#    proj = Project(id, data)
#    assert proj.field('address') == '123 goog st'  # ignored child value
#    assert proj.field('num_units') == '144'
#    # ignored more recent one from child:
#    assert proj.field('num_units_bmr') == '22'
#    assert proj.field('num_square_feet') == '2300'
