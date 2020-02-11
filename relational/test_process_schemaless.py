# Lint as: python3
from datetime import datetime
from collections import namedtuple

from relational.process_schemaless import Freshness
from relational.process_schemaless import is_seen_id
from schemaless.sources import MOHCDPipeline
from schemaless.sources import PPTS
from schemaless.sources import PTS


def test_freshness():
    newer = datetime.fromisoformat('2020-01-01')
    pts = datetime.fromisoformat('2020-02-01')
    mohcd = datetime.fromisoformat('2019-01-01')

    lines = []
    lines.append({
        'source': PPTS.NAME,
        'name': 'date_opened',
        'value': '01/01/2000',
    })
    lines.append({
        'source': PPTS.NAME,
        'name': 'date_opened',
        'value': '01/01/2010',
    })
    lines.append({
        'source': PPTS.NAME,
        'name': 'date_opened',
        'value': '01/01/2020',
    })
    lines.append({
        'source': PTS.NAME,
        'name': 'filed_date',
        'value': '01/01/2010',
    })
    lines.append({
        'source': PTS.NAME,
        'name': 'completed_date',
        'value': '02/01/2020',
    })

    # ignored because the field isn't permitted
    lines.append({
        'source': PTS.NAME,
        'name': 'arbitrary',
        'value': '02/02/2020',
    })

    # ignored, in the future
    lines.append({
        'source': PPTS.NAME,
        'name': 'date_opened',
        'value': datetime.max.strftime('%m/%d/%Y'),
    })

    # ignored because the source is unknown
    lines.append({
        'source': 'bamboozle',
        'name': 'date_opened',
        'value': '02/01/2020',
    })

    # mohcd extracts from last_updated
    lines.append({
        'last_updated': '2019-01-01',  # isoformat for last_updated
        'source': MOHCDPipeline.NAME,
        'name': 'date_opened',
        'value': '01/01/2020',
    })

    fresh = Freshness()
    for line in lines:
        fresh.update_freshness(line)

    assert fresh.freshness[PPTS.NAME] == newer
    assert fresh.freshness[PTS.NAME] == pts
    assert fresh.freshness[MOHCDPipeline.NAME] == mohcd
    assert 'bamboozle' not in fresh.freshness


def test_is_seen_id():
    seen_set = set('123')

    class FakeTable:
        ID = 'id'

        def index(self, foo):
            return 0

    RowTest = namedtuple(
        'RowTest',
        ['input', 'want'])
    tests = [
        RowTest(['1'], True),
        RowTest(['4'], False),
        RowTest(['2'], True),
    ]
    for test in tests:
        assert is_seen_id(test.input, FakeTable(), seen_set) == test.want
