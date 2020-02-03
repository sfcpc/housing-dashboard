# Lint as: python3
from datetime import datetime
from collections import defaultdict
from collections import namedtuple

from relational.project import Entry
from relational.project import NameValue
from relational.process_schemaless import extract_freshness
from relational.process_schemaless import is_seen_id


def test_extract_freshness():
    entries_map = defaultdict(list)
    newer = datetime.fromisoformat('2020-01-01')

    entries_map['uuid1'].append(Entry(
            'PRJ1', 'ppts',
            [NameValue('date_opened', '01/01/2000', newer)],
    ))
    entries_map['uuid2'].append(Entry(
            'PRJ1', 'ppts',
            [NameValue('date_opened', '01/01/2010', newer)],
    ))
    entries_map['uuid3'].append(Entry(
            'PRJ1', 'ppts',
            [NameValue('date_opened', '01/01/2020', newer)],
    ))

    # ignored because the field isn't whitelisted
    entries_map['uuid4'].append(Entry(
            'PRJ1', 'ppts',
            [NameValue('arbitrary', '02/01/2020', newer)],
    ))

    # ignored, in the future
    entries_map['uuid5'].append(Entry(
            'PRJ1', 'ppts',
            [NameValue('arbitrary', datetime.max.strftime('%m/%d/%Y'), newer)],
    ))

    # ignored because the source is unknown
    entries_map['uuid6'].append(Entry(
            'PRJ1', 'bamboozle',
            [NameValue('date_opened', '02/01/2020', newer)],
    ))

    freshness = extract_freshness(entries_map)

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
