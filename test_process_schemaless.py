# Lint as: python3
from datetime import datetime
from collections import defaultdict
from collections import namedtuple

from process_schemaless import extract_freshness
from process_schemaless import is_seen_id
from process.types import four_level_dict


def test_extract_freshness():
    data = defaultdict(lambda: four_level_dict())

    oldest = datetime.fromisoformat('2000-01-01')
    old = datetime.fromisoformat('2010-01-01')
    newer = datetime.fromisoformat('2020-01-01')

    data['uuid1']['ppts']['PRJ']['foo'] = {'last_updated': oldest}
    data['uuid1']['ppts']['PRJ']['foo'] = {'last_updated': old}
    data['uuid1']['ppts']['PRJ']['foo'] = {'last_updated': newer}

    freshness = extract_freshness(data)

    assert freshness['ppts'] == newer


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
