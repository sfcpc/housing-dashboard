# Lint as: python3
#
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Mapblklot
from schemaless.sources import MapblklotException
import schemaless.mapblklot_generator as mapblklot_gen
import pytest


def test_mapblklot_field():
    record = {'blklot': '3514098'}
    mapblklot = Mapblklot(blklot='blklot')

    # Assert that an exception is thrown when trying to use the
    # mapblklot generator without instantiating it.
    with pytest.raises(MapblklotException):
        mapblklot.get_value(record)

    mapblklot_gen.generator_instance = mapblklot_gen.MapblklotGenerator(
        'data/assessor/2020-02-18-parcels.csv.xz')
    assert(mapblklot.get_value(record) == '3514045')

    record = {'block': '3514', 'lot': '098'}
    mapblklot = Mapblklot(block='block', lot='lot')
    assert(mapblklot.get_value(record) == '3514045')

    record = {'mapblklot': 'foo'}
    mapblklot = Mapblklot(mapblklot='mapblklot')
    assert(mapblklot.get_value(record) == 'foo')


def test_permit_addenda_yield_records():
    ''' Checks that addenda summary source correctly summarizes details
    for each permit number.
    '''
    addenda_source = PermitAddendaSummary('testdata/permit-addenda.csv')
    expected_records = {
        'permit_addenda_summary_201609218371':
        {
            'permit_number': '201609218371',
            'earliest_addenda_arrival': '2016-09-21'
        },
        'permit_addenda_summary_8410366':
        {
            'permit_number': '8410366',
            'earliest_addenda_arrival': ''
        },
        'permit_addenda_summary_201810233961':
        {
            'permit_number': '201810233961',
            'earliest_addenda_arrival': '2018-10-24'
        }
    }
    for line in addenda_source.yield_records():
        assert addenda_source.foreign_key(line) in expected_records.keys()
        expected_record = expected_records[addenda_source.foreign_key(line)]
        for key, val in line.items():
            assert key in expected_record
            assert val == expected_record[key]
