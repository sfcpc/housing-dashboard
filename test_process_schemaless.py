# Lint as: python3
from datetime import datetime
from collections import defaultdict
from collections import namedtuple

from process_schemaless import extract_freshness
from process_schemaless import is_seen_id
from process.types import four_level_dict


def test_extract_freshness():
    data = defaultdict(lambda: four_level_dict())

    newer = datetime.fromisoformat('2020-01-01')

    data['uuid1']['ppts']['PRJ']['date_opened']['value'] = '01/01/2000'
    data['uuid2']['ppts']['PRJ']['date_opened']['value'] = '01/01/2010'
    data['uuid3']['ppts']['PRJ']['date_opened']['value'] = '01/01/2020'

    # ignored
    data['uuid4']['ppts']['PRJ']['arbitrary']['value'] = '02/01/2020'

    # ignored, in the future
    data['uuid5']['ppts']['PRJ']['date_opened']['value'] = \
        datetime.max.strftime('%m/%d/%Y')

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
