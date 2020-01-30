# Lint as: python3
from datetime import datetime
from collections import namedtuple

from process.project import Project
from process.types import four_level_dict
from process.generators import atleast_one_measure
from process.generators import nv_bedroom_info


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


def test_nv_bedroom_info():
    def _get_name_value(nvs, name):
        for nv in nvs:
            if name == nv.name:
                return nv.value
        return ''

    d = datetime.fromisoformat('2019-01-01')

    data1 = four_level_dict()
    data1['ppts']['PRJ']['residential_units_1br_net']['value'] = '10'
    data1['ppts']['PRJ']['residential_units_1br_net']['last_updated'] = d
    proj_normal = Project('uuid1', data1)

    data2 = four_level_dict()
    data2['ppts']['PRJ']['residential_units_adu_1br_net']['value'] = '1'
    data2['ppts']['PRJ']['residential_units_adu_1br_net']['last_updated'] = d
    proj_adu = Project('uuid2', data2)

    nvs = nv_bedroom_info(proj_normal)
    assert _get_name_value(nvs, 'residential_units_1br') == '10'

    nvs = nv_bedroom_info(proj_adu)
    assert _get_name_value(nvs, 'residential_units_adu_1br') == '1'
    assert _get_name_value(nvs, 'is_adu') == 'TRUE'
