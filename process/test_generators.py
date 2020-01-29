# Lint as: python3
from collections import namedtuple

from generators import atleast_one_measure


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
