# Lint as: python3
"""Tests for create_schemaless.py"""
import csv
from csv import DictReader
from datetime import date
import filecmp
import shutil

from schemaless.create_schemaless import dump_and_diff
from schemaless.create_schemaless import just_dump
from schemaless.create_schemaless import latest_values
import schemaless.mapblklot_generator as mapblklot_gen
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import DAInfo
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Planning
from schemaless.sources import PTS
from schemaless.sources import TCO


TESTDATA_GEN_DATE = date(2020, 1, 29)


def setup_module(module):
    if mapblklot_gen.MapblklotGeneratorSingleton.get_instance() is None:
        mapblklot_gen.init('data/assessor/2020-02-18-parcels.csv.xz')


def test_latest_values_num_entries():
    """Each unique fk gets its own entry in latest_values.
    """
    unique_fks = set()
    with open('testdata/schemaless-one.csv', 'r') as f:
        reader = DictReader(f)
        for line in reader:
            unique_fks.add(line['fk'])

    latest = latest_values('testdata/schemaless-one.csv')
    assert len(unique_fks) > 1  # Sanity check
    assert sum(map(lambda x: len(x), latest.values())) == len(unique_fks)


def test_latest_values_update(tmpdir):
    """Always use the latest value for a key"""
    # Note: tmpdir is a built-in pytest fixture that makes a temp dir
    sf = tmpdir.join("schemaless.csv")
    shutil.copyfile('testdata/schemaless-one.csv', sf)
    fk = 'planning_2016-001514PRJ'  # 1950 Mission St
    aff_key = 'number_of_affordable_units'

    latest = latest_values(sf)
    assert latest[Planning.NAME][fk][aff_key] == '157'

    # Change the units
    with open(sf, 'a') as outf:
        writer = csv.writer(outf)
        writer.writerow(
            [fk, 'planning', date.today().isoformat(),
             'number_of_affordable_units', '700'])
    latest = latest_values(sf)
    assert latest[Planning.NAME][fk][aff_key] == '700'

    # Change the units again
    with open(sf, 'a') as outf:
        writer = csv.writer(outf)
        writer.writerow(
            [fk, 'planning', date.today().isoformat(),
             'number_of_affordable_units', '100'])
    latest = latest_values(sf)
    assert latest[Planning.NAME][fk][aff_key] == '100'


def test_latest_values_no_collision():
    """Ensure we're not overwriting values for unrelated projects."""
    latest = latest_values('testdata/schemaless-one.csv')
    assert latest[Planning.NAME][
            'planning_2016-008581PRJ']['number_of_market_rate_units'] \
        != latest[Planning.NAME][
            'planning_2017-007883PRJ']['number_of_market_rate_units']


def test_just_dump(tmpdir):
    """Ensure dumping produces the expected result."""
    outfile = tmpdir.join("schemaless.csv")
    just_dump(
        [Planning('testdata/planning-one.csv'),
         PTS('testdata/pts.csv'),
         TCO('testdata/tco.csv'),
         MOHCDPipeline('testdata/mohcd-pipeline.csv'),
         MOHCDInclusionary('testdata/mohcd-inclusionary.csv'),
         PermitAddendaSummary('testdata/permit-addenda.csv'),
         AffordableRentalPortfolio('testdata/bmr.csv'),
         DAInfo('testdata/da.csv')],
        outfile,
        the_date=TESTDATA_GEN_DATE)
    assert filecmp.cmp('testdata/schemaless-one.csv', outfile)


def test_dump_and_diff(tmpdir):
    """Ensure dumping produces the expected result."""
    outfile = tmpdir.join("schemaless.csv")
    dump_and_diff(
        [Planning('testdata/planning-two.csv')],
        outfile,
        'testdata/schemaless-one.csv',
        the_date=TESTDATA_GEN_DATE)
    assert filecmp.cmp('testdata/schemaless-two.csv', outfile)
