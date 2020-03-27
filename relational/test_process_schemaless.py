# Lint as: python3
from datetime import datetime
from collections import namedtuple
import filecmp

from relational.process_schemaless import Freshness
from relational.process_schemaless import is_seen_id
from relational.process_schemaless import run
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDPipeline
from schemaless.sources import OEWDPermits
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Planning
from schemaless.sources import PTS
from schemaless.sources import TCO


def test_freshness():
    newer = datetime.fromisoformat('2020-01-01')
    pts = datetime.fromisoformat('2020-02-01')
    tco = datetime.fromisoformat('2020-02-10')
    mohcd = datetime.fromisoformat('2019-01-01')
    addenda = datetime.fromisoformat('2019-05-05')
    oewd = datetime.fromisoformat('2019-10-10')

    lines = []
    lines.append({
        'source': Planning.NAME,
        'name': 'date_opened',
        'value': '2000-01-01',
    })
    lines.append({
        'source': Planning.NAME,
        'name': 'date_opened',
        'value': '2010-01-01',
    })
    lines.append({
        'source': Planning.NAME,
        'name': 'date_opened',
        'value': '2020-01-01',
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
    lines.append({
        'source': TCO.NAME,
        'name': 'date_issued',
        'value': '2020/02/05',
    })
    lines.append({
        'source': TCO.NAME,
        'name': 'date_issued',
        'value': '2020/02/10',
    })

    # ignored because the field isn't permitted
    lines.append({
        'source': PTS.NAME,
        'name': 'arbitrary',
        'value': '02/02/2020',
    })
    lines.append({
        'source': TCO.NAME,
        'name': 'arbitrary',
        'value': '2020/05/02',
    })

    # ignored, in the future
    lines.append({
        'source': Planning.NAME,
        'name': 'date_opened',
        'value': datetime.max.strftime('%Y-%m-%d'),
    })

    # ignored because the source is unknown
    lines.append({
        'source': 'bamboozle',
        'name': 'date_opened',
        'value': '02/01/2020',
    })

    # mohcd extracts from last_updated
    lines.append({
        'last_updated': '01/01/2019 12:00:00 AM',
        'source': MOHCDPipeline.NAME,
        'name': 'date_opened',
        'value': '01/01/2020',
    })

    # oewd extracts from last_updated
    lines.append({
        'last_updated': '10/10/2019 12:00:00 AM',
        'source': OEWDPermits.NAME,
        'name': 'dbi_arrival_date',
        'value': '01/01/2020',
    })

    # permit addenda summary extracts from last_updated
    lines.append({
        'last_updated': '05/05/2019 12:00:00 AM',
        'source': PermitAddendaSummary.NAME,
        'name': 'earliest_addenda_arrival',
        'value': '04/01/2015',
    })

    # affordable rental porrtfolio extracts from last_updated
    lines.append({
        'last_updated': '01/01/2019 12:00:00 AM',
        'source': AffordableRentalPortfolio.NAME,
        'name': 'project_name',
        'value': 'the foo building',
    })

    fresh = Freshness()
    for line in lines:
        fresh.update_freshness(line)

    assert fresh.freshness[Planning.NAME] == newer
    assert fresh.freshness[PTS.NAME] == pts
    assert fresh.freshness[TCO.NAME] == tco
    assert fresh.freshness[MOHCDPipeline.NAME] == mohcd
    assert fresh.freshness[AffordableRentalPortfolio.NAME] == mohcd
    assert fresh.freshness[PermitAddendaSummary.NAME] == addenda
    assert fresh.freshness[OEWDPermits.NAME] == oewd
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


def test_run(tmpdir):
    run(schemaless_file='testdata/schemaless-two.csv',
        uuid_map_file='testdata/uuid-map-two.csv',
        parcel_data_file='data/assessor/2020-02-18-parcels.csv.xz',
        out_prefix=tmpdir)

    freshness = tmpdir.join("data_freshness.csv")
    assert filecmp.cmp('testdata/relational/data_freshness.csv', freshness)

    facts = tmpdir.join("project_facts.csv")
    assert filecmp.cmp('testdata/relational/project_facts.csv', facts)

    details = tmpdir.join("project_details.csv")
    assert filecmp.cmp('testdata/relational/project_details.csv', details)

    geo = tmpdir.join("project_geo.csv")
    assert filecmp.cmp('testdata/relational/project_geo.csv', geo)

    status_history = tmpdir.join("project_status_history.csv")
    assert filecmp.cmp('testdata/relational/project_status_history.csv',
                       status_history)

    unit_counts = tmpdir.join("project_unit_counts_full.csv")
    assert filecmp.cmp('testdata/relational/project_unit_counts_full.csv',
                       unit_counts)

    completed = tmpdir.join("project_completed_unit_counts.csv")
    assert filecmp.cmp('testdata/relational/project_completed_unit_counts.csv',
                       completed)
