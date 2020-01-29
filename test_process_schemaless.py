# Lint as: python3
from datetime import datetime
from collections import defaultdict
from collections import namedtuple

from process_schemaless import Project
from process_schemaless import atleast_one_measure
from process_schemaless import four_level_dict


def test_atleast_one_measure():
    header = ['num_units', 'num_units_bmr', 'num_square_feet']

    RowTest = namedtuple('RowTest', ['input', 'want', 'header'], defaults=[header])
    tests = [
        RowTest(['', '', ''], False), # empty row
        RowTest(['', '0', ''], True), # zero different from empty
        RowTest(['1', '2', '3'], True), # normal full row
        RowTest(['1', '2', '3'], True, ['a', 'b', 'c']), # no relevant measures to filter on
    ]
    for test in tests:
        assert atleast_one_measure(test.input, test.header) == test.want


def test_project_no_main_record():
    old = datetime.fromisoformat('2019-01-01')
    lessold = datetime.fromisoformat('2020-01-01')

    data = four_level_dict()
    data['ppts']['CUA1']['parent']['value'] = 'PRJ'
    data['ppts']['CUA1']['parent']['last_updated'] = old
    data['ppts']['CUA1']['num_units_bmr']['value'] = '22'
    data['ppts']['CUA1']['num_units_bmr']['last_updated'] = old

    data['ppts']['CUA2']['parent']['value'] = 'PRJ'
    data['ppts']['CUA2']['parent']['last_updated'] = lessold
    data['ppts']['CUA2']['num_units_bmr']['value'] = '32'
    data['ppts']['CUA2']['num_units_bmr']['last_updated'] = lessold
    data['ppts']['CUA2']['num_square_feet']['value'] = '2300'
    data['ppts']['CUA2']['num_square_feet']['last_updated'] = lessold

    proj = Project('uuid-0001', data)

    # oldest one got upgraded
    assert proj.main is not None
    assert proj.main.key == 'CUA1'
    assert len(proj.children) == 1
    assert proj.children[0].key == 'CUA2'
    assert proj.field('num_units_bmr') == '22'


def test_project_field():
    """Test tie-breaking rules for fields"""
    old = datetime.fromisoformat('2019-01-01')
    lessold = datetime.fromisoformat('2020-01-01')

    id = 'uuid-0000'
    data = four_level_dict()
    data['ppts']['PRJ']['address']['value'] = '123 goog st'
    data['ppts']['PRJ']['address']['last_updated'] = old
    data['ppts']['PRJ']['num_units']['value'] = '144'
    data['ppts']['PRJ']['num_units']['last_updated'] = old
    data['ppts']['PRJ']['num_units_bmr']['value'] = '22'
    data['ppts']['PRJ']['num_units_bmr']['last_updated'] = old

    data['ppts']['ENV']['parent']['value'] = 'PRJ'
    data['ppts']['ENV']['parent']['last_updated'] = old
    data['ppts']['ENV']['address']['value'] = '123 Gog st'
    data['ppts']['ENV']['address']['last_updated'] = old
    data['ppts']['ENV']['num_square_feet']['value'] = '2200'
    data['ppts']['ENV']['num_square_feet']['last_updated'] = old

    data['ppts']['CUA1']['parent']['value'] = 'PRJ'
    data['ppts']['CUA1']['parent']['last_updated'] = old

    data['ppts']['CUA2']['parent']['value'] = 'PRJ'
    data['ppts']['CUA2']['parent']['last_updated'] = lessold
    data['ppts']['CUA2']['num_units_bmr']['value'] = '32'
    data['ppts']['CUA2']['num_units_bmr']['last_updated'] = lessold
    data['ppts']['CUA2']['num_square_feet']['value'] = '2300'
    data['ppts']['CUA2']['num_square_feet']['last_updated'] = lessold

    proj = Project(id, data)
    assert proj.field('address') == '123 goog st'  # ignored child value
    assert proj.field('num_units') == '144'
    # ignored more recent one from child:
    assert proj.field('num_units_bmr') == '22'
    assert proj.field('num_square_feet') == '2300'
