# Lint as: python3
import pytest

from datetime import datetime
from collections import defaultdict

from process_schemaless import Project
from process_schemaless import Record

# TODO: possible code smell, need a better structure
def _four_default_dict():
    return defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(str))))

def test_project_no_main_record():
    old = datetime.fromisoformat('2019-01-01')
    lessold = datetime.fromisoformat('2020-01-01')

    data = _four_default_dict()
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
    data = _four_default_dict()
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
    assert proj.field('address') == '123 goog st' # ignored child value
    assert proj.field('num_units') == '144'
    assert proj.field('num_units_bmr') == '22' # ignored more recent one from child
    assert proj.field('num_square_feet') == '2300'

