# Lint as: python3
from schemaless.create_uuid_map import RecordGraph
from schemaless.create_uuid_map import RecordGraphBuilder
import schemaless.mapblklot_generator as mapblklot_gen


def setup_module(module):
    if mapblklot_gen.MapblklotGeneratorSingleton.get_instance() is None:
        mapblklot_gen.init('data/assessor/2020-02-18-parcels.csv.xz')


def test_address_match():
    kennedy_towers = "bmr_2013-037"
    # TODO(#76): This parent is actually wrong. The record starts after the
    # BMRs are available, and we should actually have no parents.
    parent = 'planning_2017-016475ENF'
    rgb = RecordGraphBuilder(
        RecordGraph,
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv',
        True,
    )
    rgb.build()
    assert parent in rgb.likelies[kennedy_towers]['parents']


def test_exclude_existing_parents():
    fk = 'mohcd_inclusionary_2017-034'
    rgb = RecordGraphBuilder(
        RecordGraph,
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv',
        find_likely_matches=True,
        exclude_known_likely_matches=True,
    )
    rgb.build()
    assert set([]) == set(rgb.likelies[fk]['parents'])


def test_dont_exclude_existing_parents():
    fk = 'mohcd_inclusionary_2017-034'
    rgb = RecordGraphBuilder(
        RecordGraph,
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv',
        find_likely_matches=True,
        exclude_known_likely_matches=False,
    )
    rgb.build()
    parents = [
        'planning_2015-014058CND',
        'planning_2015-014058CUA',
        'planning_2015-014058ENV',
        'planning_2015-014058PPA',
        'planning_2015-014058PRJ',
        'planning_2015-014058TDM',
        'planning_2015-014058VAR',
    ]

    assert set(parents) == set(rgb.likelies[fk]['parents'])
