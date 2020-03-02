# Lint as: python3
import csv
import sys

import schemaless.mapblklot_generator as mapblklot_gen

csv.field_size_limit(sys.maxsize)


def setup_module(module):
    if mapblklot_gen.MapblklotGeneratorSingleton.get_instance() is None:
        mapblklot_gen.init('data/assessor/2020-02-18-parcels.csv.xz')


def test_correct_mapblklot():
    gen = mapblklot_gen.MapblklotGeneratorSingleton.get_instance()
    assert(gen.find_mapblklot_for_blklot('3514098') == '3514045')
    assert(gen.find_mapblklot_for_blklot('2935015') == '2935015')
    assert(gen.find_mapblklot_for_blklot('foo') is None)
    assert(gen.find_mapblklot_for_blklot('') is None)


def test_centroid():
    gen = mapblklot_gen.MapblklotGeneratorSingleton.get_instance()
    res = gen.find_lnglat_for_blklot('0102002')
    assert res[0] == '-122.41073416433598'
    assert res[1] == '37.80034543156244'
