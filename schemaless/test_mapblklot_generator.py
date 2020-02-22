# Lint as: python3
from schemaless.mapblklot_generator import MapblklotGenerator


def test_behavior_before_instantiation():
    assert(MapblklotGenerator.get_instance() is None)


def test_one_instance():
    MapblklotGenerator('data/assessor/2020-02-18-parcels.csv.xz')

    instance = MapblklotGenerator.get_instance()
    assert(instance is not None)
    # Make sure that subsequent calls to get the instance returns the same
    # underlying instance.
    assert(instance == MapblklotGenerator.get_instance())

    # Make sure that even if someone tries to reinitialize the generator, the
    # same underlying instance is what is returned.
    MapblklotGenerator('data/assessor/2020-02-18-parcels.csv.xz')
    assert(instance == MapblklotGenerator.get_instance())


def test_correct_mapblklot():
    mapblklot_gen = MapblklotGenerator.get_instance()
    assert(mapblklot_gen.find_mapblklot_for_blklot('3514098') == '3514045')
    assert(mapblklot_gen.find_mapblklot_for_blklot('2935015') == '2935015')
    assert(mapblklot_gen.find_mapblklot_for_blklot('foo') is None)
    assert(mapblklot_gen.find_mapblklot_for_blklot('') is None)
