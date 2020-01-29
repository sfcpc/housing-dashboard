# Lint as: python3
from collections import namedtuple

from process_schemaless import is_seen_id


def test_is_seen_id():
    seen_set = set('123')

    header = ['id']
    RowTest = namedtuple('RowTest', ['input', 'want', 'header'],
                         defaults=[header])
    tests = [
        RowTest(['1'], True),
        RowTest(['4'], False),
        RowTest(['1'], False, ['idx']),
    ]
    for test in tests:
        assert is_seen_id(test.input, test.header, seen_set) == test.want
