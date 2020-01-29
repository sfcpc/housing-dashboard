# Lint as: python3
"""Tests for create_schemaless.py"""
import csv
from csv import DictReader
from datetime import date
import shutil

from create_schemaless import latest_values


def test_latest_values_num_entries():
    """Each unique fk gets its own entry in latest_values.

    TODO: Should this actually be (source, fk) pair?
    """
    unique_fks = set()
    with open('testdata/schemaless-one.csv', 'r') as f:
        reader = DictReader(f)
        for line in reader:
            unique_fks.add(line['fk'])

    latest = latest_values('testdata/schemaless-one.csv')
    assert len(unique_fks) > 1  # Sanity check
    assert len(latest) == len(unique_fks)


def test_latest_values_update(tmpdir):
    """Always use the latest value for a key"""
    # Note: tmpdir is a built-in pytest fixture that makes a temp dir
    sf = tmpdir.join("schemaless.csv")
    shutil.copyfile('testdata/schemaless-one.csv', sf)
    fk = '2016-001514PRJ'  # 1950 Mission St
    aff_key = 'affordable_units_proposed'

    latest = latest_values(sf)
    assert latest[fk][aff_key] == '157'

    # Change the units
    with open(sf, 'a') as outf:
        writer = csv.writer(outf)
        writer.writerow(
            [fk, 'ppts', date.today().isoformat(),
             'affordable_units_proposed', '700'])
    latest = latest_values(sf)
    assert latest[fk][aff_key] == '700'

    # Change the units again
    with open(sf, 'a') as outf:
        writer = csv.writer(outf)
        writer.writerow(
            [fk, 'ppts', date.today().isoformat(),
             'affordable_units_proposed', '100'])
    latest = latest_values(sf)
    assert latest[fk][aff_key] == '100'


def test_latest_values_no_collision():
    """Ensure we're not overwriting values for unrelated projects."""
    latest = latest_values('testdata/schemaless-one.csv')
    assert latest['2016-008581PRJ']['market_rate_units_proposed'] != \
        latest['2017-007883PRJ']['market_rate_units_proposed']
