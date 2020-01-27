# Lint as: python3
from datetime import date
import uuid

import pytest

from create_schemaless import RecordMetadata
from create_schemaless import _resolve_parent
from create_schemaless import _resolve_all_parents



@pytest.fixture
def metadata_no_parents():
    return {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_no_parents(metadata_no_parents):
    """No records have parents, always return self."""
    metadata = metadata_no_parents
    for (rid, record) in metadata.items():
        assert _resolve_parent(metadata, rid) == record


def test_resolve_all_parents_no_parents(metadata_no_parents):
    """No records have parents, each gets a new UUID."""
    metadata = _resolve_all_parents(metadata_no_parents)
    uuids = set(md.uuid for md in metadata.values())
    assert len(uuids) == len(metadata_no_parents)


@pytest.fixture
def metadata_one_parent():
    return {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_one_parent(metadata_one_parent):
    """If parent is defined, return that record."""
    metadata = metadata_one_parent
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['2']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_all_parents_one_parent(metadata_one_parent):
    """If parent is defined, use the parent's UUID."""
    metadata = _resolve_all_parents(metadata_one_parent)
    uuids = set(md.uuid for md in metadata.values())
    # One fewer UUIDs than records, because the child uses the parent UUID
    assert len(uuids) == len(metadata_one_parent) - 1
    assert metadata['1'].uuid == metadata['2'].uuid
    assert metadata['2'].uuid != metadata['3'].uuid
    assert metadata['2'].uuid != metadata['4'].uuid


@pytest.fixture
def metadata_chained_parent():
    return {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['3']),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_chained_parent(metadata_chained_parent):
    """Walk up the full parent chain to the root."""
    metadata = metadata_chained_parent
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            # This is because we walk *all* the way up to the root
            expected = metadata['3']
        elif rid == '2':
            expected = metadata['3']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_all_parents_chained_parent(metadata_chained_parent):
    """Walk up the full parent chain to the root, and use that UUID."""
    metadata = _resolve_all_parents(metadata_chained_parent)
    uuids = set(md.uuid for md in metadata.values())
    # Two fewer UUIDs than records, because the child uses the root parent UUID
    assert len(uuids) == len(metadata_chained_parent) - 2
    assert metadata['1'].uuid == metadata['3'].uuid
    assert metadata['2'].uuid == metadata['3'].uuid
    assert metadata['2'].uuid != metadata['4'].uuid
    assert metadata['3'].uuid != metadata['4'].uuid


@pytest.fixture
def metadata_multiple_parents():
    return {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2', '3']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_multiple_parents(metadata_multiple_parents):
    """Choose the youngest parent."""
    metadata = metadata_multiple_parents
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['3']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_all_parents_multiple_parents(metadata_multiple_parents):
    """Choose the youngest parent and use that UUID."""
    metadata = _resolve_all_parents(metadata_multiple_parents)
    uuids = set(md.uuid for md in metadata.values())
    # One fewer UUIDs than records, because the child uses the youngest parent
    assert len(uuids) == len(metadata_multiple_parents) - 1
    assert metadata['1'].uuid == metadata['3'].uuid
    assert metadata['2'].uuid != metadata['3'].uuid
    assert metadata['2'].uuid != metadata['4'].uuid
    assert metadata['3'].uuid != metadata['4'].uuid


@pytest.fixture
def metadata_multiple_chained_parents():
    return {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2', '3']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=['4']),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_multiple_chained_parents(
        metadata_multiple_chained_parents):
    """Choose the youngest parent in the full chain."""
    metadata = metadata_multiple_chained_parents
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['4']
        if rid == '3':
            expected = metadata['4']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_all_parents_multiple_chained_parents(
        metadata_multiple_chained_parents):
    """Choose the youngest parent in the full chain and use that UUID."""
    metadata = _resolve_all_parents(metadata_multiple_chained_parents)
    uuids = set(md.uuid for md in metadata.values())
    # Two fewer UUIDs than records, because the child uses the root parent UUID
    assert len(uuids) == len(metadata_multiple_chained_parents) - 2
    assert metadata['1'].uuid == metadata['4'].uuid
    assert metadata['2'].uuid != metadata['3'].uuid
    assert metadata['3'].uuid == metadata['4'].uuid


@pytest.fixture
def metadata_missing_parent():
    return {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['12', '13']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_missing_parent(metadata_missing_parent):
    """If no parents be found, return self."""
    metadata = metadata_missing_parent
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['1']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_all_parents_missing_parent(metadata_missing_parent):
    """If no parents be found, generate a new UUID."""
    metadata = _resolve_all_parents(metadata_missing_parent)
    uuids = set(md.uuid for md in metadata.values())
    assert len(uuids) == len(metadata_missing_parent)


@pytest.fixture
def metadata_one_missing_parent():
    return {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2', '12', '13']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_one_missing_parent(metadata_one_missing_parent):
    """If any parents can be found, operate only on those parents."""
    metadata = metadata_one_missing_parent
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['2']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_all_parents_one_missing_parent(metadata_one_missing_parent):
    """If any parent can be found, use that UUID."""
    metadata = _resolve_all_parents(metadata_one_missing_parent)
    uuids = set(md.uuid for md in metadata.values())
    # One fewer UUIDs than records, because the child uses the one parent
    # it can find
    assert len(uuids) == len(metadata_one_missing_parent) - 1
    assert metadata['1'].uuid == metadata['2'].uuid
    assert metadata['2'].uuid != metadata['3'].uuid
    assert metadata['2'].uuid != metadata['4'].uuid
    assert metadata['3'].uuid != metadata['4'].uuid


@pytest.fixture
def metadata_uuid_exit_early():
    return {
        '1': RecordMetadata(
            uuid=uuid.uuid4(),
            date_opened=date(2020, 1, 1),
            parents=['2']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }


def test_resolve_parent_uuid_exit_early(metadata_uuid_exit_early):
    """If uuid is already populated, don't keep traversing.

    TODO: Was this the right choice? Is this the correct way to handle this?
    Can this situation ever happen (and how)? This is done to handle this
    situation: a child record is present in a day's data file but its parent
    isn't. So we give the child a new uuid. On the next day we get the parent,
    but our schemaless file already has a uuid for the child. Now what?

    The options are:
        1. Continue using the previous day's uuid
        2. Use the new parent's uuid
            a. Optionally, add a 'old_uuid,<val>' entry in the schemaless file
    """
    metadata = metadata_uuid_exit_early
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['1']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_all_parents_uuid_exit_early(metadata_uuid_exit_early):
    """If uuid is already populated, don't generate a new UUID."""
    metadata = _resolve_all_parents(metadata_uuid_exit_early)
    uuids = set(md.uuid for md in metadata.values())
    assert len(uuids) == len(metadata_uuid_exit_early)
    assert metadata['1'].uuid != metadata['2'].uuid
