# Lint as: python3
import schemaless.mapblklot_generator as mapblklot_gen


def setup_module(module):
    if mapblklot_gen.MapblklotGeneratorSingleton.get_instance() is None:
        mapblklot_gen.init('data/assessor/2020-02-18-parcels.csv.xz')


def test_correct_mapblklot():
    gen = mapblklot_gen.MapblklotGeneratorSingleton.get_instance()
    assert(gen.find_mapblklot_for_blklot('3514098') == '3514045')
    assert(gen.find_mapblklot_for_blklot('2935015') == '2935015')
    assert(gen.find_mapblklot_for_blklot('foo') is None)
    assert(gen.find_mapblklot_for_blklot('') is None)
