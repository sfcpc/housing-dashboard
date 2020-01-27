# Lint as: python3
from datetime import date
import uuid

import pytest

from create_schemaless import RecordMetadata
from create_schemaless import _resolve_parent
from create_schemaless import _resolve_all_parents



def test_resolve_parent_no_parents():
    """No records have parents, always return self."""
    metadata = {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }
    for (rid, record) in metadata.items():
        assert _resolve_parent(metadata, rid) == record


def test_resolve_parent_one_parent():
    """If parent is defined, return that record."""
    metadata = {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['2']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_parent_chained_parent():
    """Walk up the full parent chain to the root."""
    metadata = {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['3']),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            # This is because we walk *all* the way up to the root
            expected = metadata['3']
        elif rid == '2':
            expected = metadata['3']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_parent_multiple_parent():
    """Choose the youngest parent."""
    metadata = {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2', '3']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['3']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_parent_multiple_chained_parent():
    """Choose the youngest parent in the full chain."""
    metadata = {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2', '3']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=['4']),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['4']
        if rid == '3':
            expected = metadata['4']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_parent_missing_parent():
    """If no parents be found, return self."""
    metadata = {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['12', '13']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['1']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_parent_one_missing_parent():
    """If any parents be found, operate only on those parents."""
    metadata = {
        '1': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=['2', '12', '13']),
        '2': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 1), parents=[]),
        '3': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 2), parents=[]),
        '4': RecordMetadata(
            uuid=None, date_opened=date(2020, 1, 3), parents=[]),
    }
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['2']
        assert _resolve_parent(metadata, rid) == expected


def test_resolve_parent_uuid_exit_early():
    """If uuid is already populated, don't keep traversing.

    TODO: Was this the right choice? Is this the correct way to handle this?
    Can this situation ever happen (and how)?
    """
    metadata = {
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
    for (rid, record) in metadata.items():
        expected = record
        if rid == '1':
            expected = metadata['1']
        assert _resolve_parent(metadata, rid) == expected
