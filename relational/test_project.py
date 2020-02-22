# Lint as: python3
from datetime import datetime

import pytest

from relational.project import Entry
from relational.project import NameValue
from relational.project import Project
from schemaless.create_uuid_map import Node
from schemaless.create_uuid_map import RecordGraph
from schemaless.sources import Planning


@pytest.fixture
def basic_entries():
    old = datetime.fromisoformat('2019-01-01')
    lessold = datetime.fromisoformat('2020-01-01')

    e = []
    e.append(Entry('1', 'planning', [NameValue('num_units_bmr', '22', old)]))
    e.append(Entry('2',
                   'planning',
                   [NameValue('num_units_bmr', '32', lessold),
                    NameValue('num_square_feet', '2300', old),
                    NameValue('residential_units_1br', '1', old)]))
    e.append(Entry('3',
                   'planning',
                   [NameValue('num_square_feet', '2100', lessold)]))
    return e


@pytest.fixture
def multi_source_entries():
    old = datetime.fromisoformat('2019-01-01')
    lessold = datetime.fromisoformat('2020-01-01')

    e = []
    e.append(Entry('1', 'planning', [NameValue('num_units_bmr', '22', old)]))
    e.append(Entry('2',
                   'pts',
                   [NameValue('num_units_bmr', '32', lessold),
                    NameValue('num_square_feet', '2300', old),
                    NameValue('residential_units_1br', '1', old)]))
    e.append(Entry('3',
                   'planning',
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


@pytest.fixture
def rootless_graph():
    rg = RecordGraph()
    rg.add(Node(record_id='2', parents=['4']))
    rg.add(Node(record_id='3', parents=['4']))
    return rg


def test_project_simple_case(basic_entries, basic_graph):
    proj = Project('uuid-0001', basic_entries, basic_graph)
    assert len(proj.roots) == 1
    assert len(proj.roots['planning']) == 1
    assert proj.roots['planning'][0].fk == '1'

    # only source data from the parent even if present in child
    assert proj.field('num_units_bmr', Planning.NAME) == '22'

    # pull data from the child since not present on the parent
    assert proj.field('residential_units_1br', Planning.NAME) == '1'

    # pull data from the latest child
    assert proj.field('num_square_feet', Planning.NAME) == '2100'

    # pull data from an earlier child because of the predicate
    assert proj.field('num_square_feet',
                      Planning.NAME,
                      entry_predicate=[('residential_units_1br',
                                        lambda x: True)]) == '2300'

    # nothing because the predicate value doesn't match
    assert proj.field('num_square_feet',
                      Planning.NAME,
                      entry_predicate=[('residential_units_1br',
                                        lambda x: x == '2')]) == ''

    # pull data from the earlier child because the predicate value DOES match
    assert proj.field('num_square_feet',
                      Planning.NAME,
                      entry_predicate=[('residential_units_1br',
                                        lambda x: x == '1')]) == '2300'


def test_project_no_main_record(basic_entries, rootless_graph):
    proj = Project('uuid-0001', basic_entries, rootless_graph)

    # oldest child got updated
    assert len(proj.roots) == 1
    assert len(proj.roots['planning']) == 1
    assert proj.roots['planning'][0].fk == '2'

    # pull data from oldest child, which got upgraded to be main
    assert proj.field('num_square_feet', Planning.NAME) == '2300'


def test_project_planning_and_pts(multi_source_entries, basic_graph):
    proj = Project('uuid-0001', multi_source_entries, basic_graph)
    assert len(proj.roots) == 1
    assert len(proj.roots['planning']) == 1
    assert proj.roots['planning'][0].fk == '1'


def test_entry():
    old = datetime.fromisoformat('2019-01-01')
    lessold = datetime.fromisoformat('2020-01-01')
    leastold = datetime.fromisoformat('2020-02-01')

    e = Entry(
        '2',
        'planning', [
            NameValue('num_units_bmr', '32', lessold),
            NameValue('num_square_feet', '2200', lessold),
            NameValue('residential_units_1br', '-1', leastold),
            NameValue('num_square_feet', '2300', old),
            NameValue('residential_units_1br', '1', old),
        ])

    assert e.oldest_name_value() == old
    assert e.num_name_values() == 5
    assert e.get_latest('num_units_bmr') == ('32', lessold)
    assert e.get_latest('num_square_feet') == ('2200', lessold)
    assert e.get_latest('residential_units_1br') == ('-1', leastold)
    assert e.latest_name_values() == {
            'num_units_bmr': '32',
            'num_square_feet': '2200',
            'residential_units_1br': '-1'
    }
