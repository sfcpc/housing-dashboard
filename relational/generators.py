# Lint as: python3
"""Contains data and name-value generators for processing schema-less CSV.
"""
import re

from collections import namedtuple

from schemaless.sources import PPTS

Field = namedtuple('Field',
                   ['name', 'value', 'always_treat_as_empty'],
                   defaults=['', '', False])

OutputNameValue = namedtuple('OutputNameValue',
                             ['name', 'value', 'data_source'],
                             defaults=['', '', ''])


def gen_id(proj):
    return [Field('id', proj.id, True)]


def gen_facts(proj):
    result = [Field()] * 5

    if proj.field('address', PPTS.NAME) != '':
        result[0] = Field('address', proj.field('address', PPTS.NAME))
        result[1] = Field('applicant', '')
        result[2] = Field('supervisor_district', '')
        result[3] = Field('permit_authority', 'planning')
        result[4] = Field('permit_authority_id', proj.field('fk', PPTS.NAME))

    return result


def gen_units(proj):
    result = [Field()] * 4

    # TODO: how to handle cases where better numbers exist from dbi
    # TODO: how to handle cases where prop - existing != net ?
    result[0] = Field('net_num_units',
                      proj.field('market_rate_units_net', PPTS.NAME))
    result[1] = Field('net_num_units_data',
                      'planning' if result[0].value != '' else '',
                      True)

    result[2] = Field('net_num_units_bmr',
                      proj.field('affordable_units_net', PPTS.NAME))
    result[3] = Field('net_num_units_bmr_data',
                      'planning' if result[2].value != '' else '',
                      True)

    return result


def nv_geom(proj):
    if proj.field('the_geom', PPTS.NAME) != '':
        return [OutputNameValue('geom',
                                proj.field('the_geom', PPTS.NAME),
                                'planning')]

    return []


def nv_all_units(proj):
    # TODO: more useful when we have more than just one data source
    result = []
    if proj.field('market_rate_units_net', PPTS.NAME):
        result.append(OutputNameValue(
            'net_num_units',
            proj.field('market_rate_units_net', PPTS.NAME),
            'planning'))
    if proj.field('affordable_units_net', PPTS.NAME):
        result.append(OutputNameValue(
            'net_num_units_bmr',
            proj.field('affordable_units_net', PPTS.NAME),
            'planning'))
    return result


def nv_bedroom_info(proj):
    is_adu = False

    def _crunch_number(prefix):
        nonlocal is_adu
        net = 0
        ok = False
        try:
            net = str(int(proj.field(prefix + '_net', PPTS.NAME)))
            ok = True

            if re.search('_adu_', prefix):
                is_adu = True
        except ValueError:
            pass

        return (net, ok)

    result = []
    for field in ['residential_units_adu_studio',
                  'residential_units_adu_1br',
                  'residential_units_adu_2br',
                  'residential_units_adu_3br',
                  'residential_units_studio',
                  'residential_units_1br',
                  'residential_units_2br',
                  'residential_units_3br',
                  'residential_units_micro',
                  'residential_units_sro']:
        (net, ok) = _crunch_number(field)
        if ok:
            result.append(OutputNameValue(field, net, 'planning'))

    result.append(OutputNameValue('is_adu',
                                  'TRUE' if is_adu else 'FALSE',
                                  'planning'))

    return result


def nv_square_feet(proj):
    if proj.field('residential_sq_ft_net', PPTS.NAME) != '':
        return [OutputNameValue('net_num_square_feet',
                                proj.field('residential_sq_ft_net', PPTS.NAME),
                                'planning')]
    return []


def atleast_one_measure(row, header):
    atleast_one = False
    seen_measure = False
    for (value, name) in zip(row, header):
        if (name == 'net_num_units' or
                name == 'net_num_units_bmr' or
                name == 'net_num_square_feet'):
            seen_measure = True
            if value != '':
                atleast_one = True
                break
    return not seen_measure or atleast_one
