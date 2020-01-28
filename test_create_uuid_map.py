# Lint as: python3
from datetime import date
import uuid

import pytest

from create_uuid_map import RecordGraph
from create_uuid_map import Node


@pytest.fixture
def graph_no_parents():
    rg = RecordGraph()
    rg.add(Node(record_id='1', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_no_parents(graph_no_parents):
    """No records have parents, always return self."""
    graph = graph_no_parents
    for (rid, record) in graph.items():
        assert graph._resolve_parent(rid) == record


def test_assign_uuids_no_parents(graph_no_parents):
    """No records have parents, each gets a new UUID."""
    graph_no_parents._assign_uuids()
    graph = graph_no_parents
    uuids = set(md.uuid for md in graph.values())
    assert len(uuids) == len(graph)


@pytest.fixture
def graph_one_parent():
    rg = RecordGraph()
    rg.add(Node(
            record_id='1', date_opened=date(2020, 1, 1), parents=['2']))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_one_parent(graph_one_parent):
    """If parent is defined, return that record."""
    graph = graph_one_parent
    for (rid, record) in graph._nodes.items():
        expected = record
        if rid == '1':
            expected = graph.get('2')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_one_parent(graph_one_parent):
    """If parent is defined, use the parent's UUID."""
    graph_one_parent._assign_uuids()
    graph = graph_one_parent
    uuids = set(md.uuid for md in graph.values())
    # One fewer UUIDs than records, because the child uses the parent UUID
    assert len(uuids) == len(graph) - 1
    assert graph.get('1').uuid == graph.get('2').uuid
    assert graph.get('2').uuid != graph.get('3').uuid
    assert graph.get('2').uuid != graph.get('4').uuid


@pytest.fixture
def graph_one_child():
    rg = RecordGraph()
    rg.add(Node(
            record_id='1', date_opened=date(2020, 1, 1), children=['2']))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_one_child(graph_one_child):
    """No records have parents, always return self."""
    graph = graph_one_child
    for (rid, record) in graph.items():
        expected = record
        if rid == '2':
            expected = graph.get('1')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_one_child(graph_one_child):
    """No records have parents, each gets a new UUID."""
    graph_one_child._assign_uuids()
    graph = graph_one_child
    uuids = set(md.uuid for md in graph.values())
    assert len(uuids) == len(graph) - 1
    assert graph.get('2').uuid == graph.get('1').uuid
    assert graph.get('1').uuid != graph.get('3').uuid
    assert graph.get('2').uuid != graph.get('3').uuid
    assert graph.get('2').uuid != graph.get('4').uuid


@pytest.fixture
def graph_one_parent_one_child():
    rg = RecordGraph()
    rg.add(Node(
        record_id='1',
        date_opened=date(2020, 1, 1),
        parents=['4'],
        children=['2'],
    ))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_one_child(graph_one_parent_one_child):
    """No records have parents, always return self."""
    graph = graph_one_parent_one_child
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            expected = graph.get('4')
        elif rid == '2':
            expected = graph.get('4')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_one_parent_one_child(
        graph_one_parent_one_child):
    """No records have parents, each gets a new UUID."""
    graph_one_parent_one_child._assign_uuids()
    graph = graph_one_parent_one_child
    uuids = set(md.uuid for md in graph.values())
    assert len(uuids) == len(graph) - 2
    assert graph.get('2').uuid == graph.get('1').uuid
    assert graph.get('1').uuid == graph.get('4').uuid
    assert graph.get('3').uuid != graph.get('4').uuid


@pytest.fixture
def graph_chained_parent():
    rg = RecordGraph()
    rg.add(Node(
        record_id='1', date_opened=date(2020, 1, 1), parents=['2']))
    rg.add(Node(
        record_id='2', date_opened=date(2020, 1, 1), parents=['3']))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_chained_parent(graph_chained_parent):
    """Walk up the full parent chain to the root."""
    graph = graph_chained_parent
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            # This is because we walk *all* the way up to the root
            expected = graph.get('3')
        elif rid == '2':
            expected = graph.get('3')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_chained_parent(graph_chained_parent):
    """Walk up the full parent chain to the root, and use that UUID."""
    graph_chained_parent._assign_uuids()
    graph = graph_chained_parent
    uuids = set(md.uuid for md in graph.values())
    # Two fewer UUIDs than records, because the child uses the root parent UUID
    assert len(uuids) == len(graph) - 2
    assert graph.get('1').uuid == graph.get('3').uuid
    assert graph.get('2').uuid == graph.get('3').uuid
    assert graph.get('2').uuid != graph.get('4').uuid
    assert graph.get('3').uuid != graph.get('4').uuid


@pytest.fixture
def graph_chained_child():
    rg = RecordGraph()
    rg.add(Node(
        record_id='1', date_opened=date(2020, 1, 1), children=['2']))
    rg.add(Node(
        record_id='2', date_opened=date(2020, 1, 1), children=['3']))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_chained_child(graph_chained_child):
    """Walk up the full parent chain to the root."""
    graph = graph_chained_child
    for (rid, record) in graph.items():
        expected = record
        if rid == '3':
            # This is because we walk *all* the way up to the root
            expected = graph.get('1')
        elif rid == '2':
            expected = graph.get('1')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_chained_child(graph_chained_child):
    """Walk up the full parent chain to the root, and use that UUID."""
    graph_chained_child._assign_uuids()
    graph = graph_chained_child
    uuids = set(md.uuid for md in graph.values())
    # Two fewer UUIDs than records, because the child uses the root parent UUID
    assert len(uuids) == len(graph) - 2
    assert graph.get('3').uuid == graph.get('1').uuid
    assert graph.get('2').uuid == graph.get('1').uuid
    assert graph.get('1').uuid != graph.get('4').uuid


@pytest.fixture
def graph_multiple_parents():
    rg = RecordGraph()
    rg.add(Node(
        record_id='1', date_opened=date(2020, 1, 1), parents=['2', '3']))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_multiple_parents(graph_multiple_parents):
    """Choose the youngest parent."""
    graph = graph_multiple_parents
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            expected = graph.get('3')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_multiple_parents(graph_multiple_parents):
    """Choose the youngest parent and use that UUID."""
    graph_multiple_parents._assign_uuids()
    graph = graph_multiple_parents
    uuids = set(md.uuid for md in graph.values())
    # One fewer UUIDs than records, because the child uses the youngest parent
    assert len(uuids) == len(graph) - 1
    assert graph.get('1').uuid == graph.get('3').uuid
    assert graph.get('2').uuid != graph.get('3').uuid
    assert graph.get('2').uuid != graph.get('4').uuid
    assert graph.get('3').uuid != graph.get('4').uuid


@pytest.fixture
def graph_multiple_chained_parents():
    rg = RecordGraph()
    rg.add(Node(
        record_id='1', date_opened=date(2020, 1, 1), parents=['2', '3']))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(
        record_id='3', date_opened=date(2020, 1, 2), parents=['4']))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_multiple_chained_parents(
        graph_multiple_chained_parents):
    """Choose the youngest parent in the full chain."""
    graph = graph_multiple_chained_parents
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            expected = graph.get('4')
        if rid == '3':
            expected = graph.get('4')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_multiple_chained_parents(
        graph_multiple_chained_parents):
    """Choose the youngest parent in the full chain and use that UUID."""
    graph_multiple_chained_parents._assign_uuids()
    graph = graph_multiple_chained_parents
    uuids = set(md.uuid for md in graph.values())
    # Two fewer UUIDs than records, because the child uses the root parent UUID
    assert len(uuids) == len(graph) - 2
    assert graph.get('1').uuid == graph.get('4').uuid
    assert graph.get('2').uuid != graph.get('3').uuid
    assert graph.get('3').uuid == graph.get('4').uuid


@pytest.fixture
def graph_missing_parent():
    rg = RecordGraph()
    rg.add(Node(
        record_id='1', date_opened=date(2020, 1, 1), parents=['12', '13']))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_missing_parent(graph_missing_parent):
    """If no parents be found, return self."""
    graph = graph_missing_parent
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            expected = graph.get('1')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_missing_parent(graph_missing_parent):
    """If no parents be found, generate a new UUID."""
    graph_missing_parent._assign_uuids()
    graph = graph_missing_parent
    uuids = set(md.uuid for md in graph.values())
    assert len(uuids) == len(graph)


@pytest.fixture
def graph_one_missing_parent():
    rg = RecordGraph()
    rg.add(Node(
        record_id='1',
        date_opened=date(2020, 1, 1),
        parents=['2', '12', '13']))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_one_missing_parent(graph_one_missing_parent):
    """If any parents can be found, operate only on those parents."""
    graph = graph_one_missing_parent
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            expected = graph.get('2')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_one_missing_parent(graph_one_missing_parent):
    """If any parent can be found, use that UUID."""
    graph_one_missing_parent._assign_uuids()
    graph = graph_one_missing_parent
    uuids = set(md.uuid for md in graph.values())
    # One fewer UUIDs than records, because the child uses the one parent
    # it can find
    assert len(uuids) == len(graph) - 1
    assert graph.get('1').uuid == graph.get('2').uuid
    assert graph.get('2').uuid != graph.get('3').uuid
    assert graph.get('2').uuid != graph.get('4').uuid
    assert graph.get('3').uuid != graph.get('4').uuid


@pytest.fixture
def graph_uuid_new_parent():
    rg = RecordGraph()
    rg.add(Node(
        uuid=uuid.uuid4(),
        record_id='1',
        date_opened=date(2020, 1, 1),
        parents=['2']))
    rg.add(Node(record_id='2', date_opened=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_uuid_new_parent(graph_uuid_new_parent):
    """If uuid is already but the parent isn't, use the child's uuid."""
    graph = graph_uuid_new_parent
    orig_uuid = graph.get('1').uuid
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            expected = graph.get('2')
        assert graph._resolve_parent(rid) == expected
    assert graph.get('1').uuid == orig_uuid


def test_assign_uuids_uuid_new_parent(graph_uuid_new_parent):
    """If uuid is already populated, use the child's uuid."""
    graph_uuid_new_parent._assign_uuids()
    graph = graph_uuid_new_parent
    uuids = set(md.uuid for md in graph.values())
    assert len(uuids) == len(graph) - 1
    assert graph.get('1').uuid == graph.get('2').uuid


@pytest.fixture
def graph_uuid_reassign_parent():
    rg = RecordGraph()
    puid = uuid.uuid4()
    rg.add(Node(
        uuid=puid,
        record_id='1',
        date_opened=date(2020, 1, 1),
        parents=['2']))
    rg.add(Node(
        uuid=uuid.uuid4(),
        record_id='2',
        date_opened=date(2020, 1, 1)))
    # This simulates the case where record_id=3 used to be a parent of 1
    rg.add(Node(
        uuid=puid,
        record_id='3',
        date_opened=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date_opened=date(2020, 1, 3)))
    return rg


def test_resolve_parent_uuid_reassign_parent(graph_uuid_reassign_parent):
    """If uuid is already set but the parent isn't, use the child's uuid."""
    graph = graph_uuid_reassign_parent
    for (rid, record) in graph.items():
        expected = record
        if rid == '1':
            expected = graph.get('2')
        assert graph._resolve_parent(rid) == expected


def test_assign_uuids_uuid_reassign_parent(graph_uuid_reassign_parent):
    """If uuid is already populated, use the child's uuid."""
    graph = graph_uuid_reassign_parent
    orig_uuid = graph.get('1').uuid
    graph_uuid_reassign_parent._assign_uuids()
    uuids = set(md.uuid for md in graph.values())
    assert len(uuids) == len(graph) - 1
    assert graph.get('1').uuid == graph.get('2').uuid
    assert graph.get('1').uuid != orig_uuid
    assert graph.get('2').uuid != orig_uuid
    assert graph.get('3').uuid == orig_uuid
