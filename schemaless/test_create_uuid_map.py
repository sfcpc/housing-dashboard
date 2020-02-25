# Lint as: python3
from datetime import date
import uuid

import pytest

from schemaless.create_uuid_map import RecordGraph
from schemaless.create_uuid_map import Node
import schemaless.mapblklot_generator as mapblklot_gen


def setup_module(module):
    mapblklot_gen.generator_instance = mapblklot_gen.MapblklotGenerator(
        'data/assessor/2020-02-18-parcels.csv.xz')


def teardown_module(module):
    mapblklot_gen.generator_instance = None


@pytest.fixture
def graph_no_parents():
    rg = RecordGraph()
    rg.add(Node(record_id='1', date=date(2020, 1, 1)))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
            record_id='1', date=date(2020, 1, 1), parents=['2']))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
            record_id='1', date=date(2020, 1, 1), children=['2']))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        date=date(2020, 1, 1),
        parents=['4'],
        children=['2'],
    ))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
    return rg


def test_resolve_parent_one_parent_one_child(graph_one_parent_one_child):
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
        record_id='1', date=date(2020, 1, 1), parents=['2']))
    rg.add(Node(
        record_id='2', date=date(2020, 1, 1), parents=['3']))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        record_id='1', date=date(2020, 1, 1), children=['2']))
    rg.add(Node(
        record_id='2', date=date(2020, 1, 1), children=['3']))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        record_id='1', date=date(2020, 1, 1), parents=['2', '3']))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        record_id='1', date=date(2020, 1, 1), parents=['2', '3']))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(
        record_id='3', date=date(2020, 1, 2), parents=['4']))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        record_id='1', date=date(2020, 1, 1), parents=['12', '13']))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        date=date(2020, 1, 1),
        parents=['2', '12', '13']))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        date=date(2020, 1, 1),
        parents=['2']))
    rg.add(Node(record_id='2', date=date(2020, 1, 1)))
    rg.add(Node(record_id='3', date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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
        date=date(2020, 1, 1),
        parents=['2']))
    rg.add(Node(
        uuid=uuid.uuid4(),
        record_id='2',
        date=date(2020, 1, 1)))
    # This simulates the case where record_id=3 used to be a parent of 1
    rg.add(Node(
        uuid=puid,
        record_id='3',
        date=date(2020, 1, 2)))
    rg.add(Node(record_id='4', date=date(2020, 1, 3)))
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


# Tests below use the data in the 'testdata' directory
def test_1950_mission_records_linked():
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')

    verify_records_linked(rg, [
        'planning_2016-001514PRJ',
        'planning_2016-001514PPA',
        'planning_2016-001514CUA',
        'planning_2016-001514ENV',
        'planning_2016-001514MCM',
        'pts_1438278158065',
        'mohcd_pipeline_2013-046',
        'permit_addenda_summary_201609218371'
    ])


def test_planning_child_1950_mission_just_parent():
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')
    child = rg.get('planning_2016-001514GPR')
    parent = rg.get('planning_2016-001514PPA')
    grandparent = rg.get('planning_2016-001514PRJ')
    assert len(child.parents) == 1
    assert child.record_id in parent.children
    assert parent.record_id in child.parents
    assert child.uuid == parent.uuid
    assert child.uuid == grandparent.uuid


def test_link_pts_records_without_planning():
    pts_records = [
        'pts_129220091307',
        'pts_1292201488027'
    ]
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')
    verify_records_linked(rg, pts_records)


def test_link_pts_group_without_planning():
    # The following pts records have the same 'mapblklot', 'filed_date', and
    # 'proposed_use', and therefore belong to the same grouping. Thus, they
    # must all be assigned the same UUID.
    pts_records = [
        'pts_1572972516074',  # permit no: 201910225142
        'pts_1572988516074',  # permit no: 201910225150
        'pts_1572991516074',  # permit no: 201910225151
        'pts_1572992516074',  # permit no: 201910225152
        'pts_1572996516074',  # permit no: 201910225153
        'pts_1573002516074',  # permit no: 201910225154
        'pts_1573004516074',  # permit no: 201910225155
    ]
    # Linking on building permit number 201705318009
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')
    verify_records_linked(rg, pts_records)


def test_link_pts_group_with_ppts():
    # Lists permit 201712085886 as 'related_building_permit'
    prj_fk_1 = 'planning_2017-016047PRJ'

    # Lists permit 201712085886 as 'related_building_permit'
    prj_fk_2 = 'planning_2017-016045PRJ'

    # These pts records have the same 'mapblklot', 'filed_date', and
    # 'proposed_use' and therefore belong to the same 'permit group'.
    # Thus, they must all be assigned the same UUID.
    pts_records = [
        # permit no: 201712085881
        'pts_1489866510069',
        # permit no: 201712085886 (ppts_2017-016047PRJ &
        # ppts_2017-016045PRJ refer to this permit no).
        'pts_1489855510068'
    ]

    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')

    # Verify that the permits in the group have the same uuid.
    verify_records_linked(rg, pts_records)
    permit_group_uuid = rg.get(pts_records[0]).uuid

    # Verify that the permit group has the same uuid as at least one of
    # the prjs that list permit no 201712085886 as a
    # 'related_building_permit'.
    prj_fk_1_uuid = rg.get(prj_fk_1).uuid
    prj_fk_2_uuid = rg.get(prj_fk_2).uuid

    assert(prj_fk_1_uuid == permit_group_uuid or
           prj_fk_2_uuid == permit_group_uuid)


def test_link_pts_to_planning_records():
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')
    verify_records_linked(rg, [
        'planning_2017-007883PRJ',
        'pts_1465081108606',
        'pts_1465082390978']
    )
    verify_records_linked(rg, ['planning_2017-006969PRL', 'pts_1465580423638'])


def test_tco_link():
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')
    verify_records_linked(rg, [
        'planning_2017-006823PRJ',
        'pts_1492183510316',
        'pts_1464175214172',
        'tco_201705237369_2018-05-01'
    ])


def test_mohcd_records_link_with_prj():
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')
    verify_records_linked(rg, [
        'planning_2015-014058PRJ',
        'planning_2015-014058CND',
        'planning_2015-014058CUA',
        'planning_2015-014058ENV',
        'planning_2015-014058PPA',
        'planning_2015-014058TDM',
        'planning_2015-014058VAR',
        'mohcd_pipeline_2017-034',
        'mohcd_inclusionary_2017-034'
    ])


def test_mohcd_records_link_without_prj():
    rg = RecordGraph.from_files(
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv')
    verify_records_linked(rg, [
        'mohcd_pipeline_2016-023',
        'mohcd_inclusionary_2016-023'])


def verify_records_linked(rg, fks):
    for fk in fks[1:]:
        assert rg.get(fks[0]).uuid == rg.get(fk).uuid
