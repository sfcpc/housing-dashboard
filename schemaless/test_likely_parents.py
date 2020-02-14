# Lint as: python3
from schemaless.create_uuid_map import RecordGraph
from schemaless.create_uuid_map import RecordGraphBuilder


def test_address_match():
    kennedy_towers = "bmr_2013-037"
    # TODO(#76): This parent is actually wrong. The record starts after the
    # BMRs are available, and we should actually have no parents.
    parent = 'ppts_2017-016475ENF'
    rgb = RecordGraphBuilder(
        RecordGraph,
        'testdata/schemaless-one.csv',
        'testdata/uuid-map-one.csv',
        True,
    )
    rgb.build()
    assert parent in rgb.likelies[kennedy_towers]['parents']
