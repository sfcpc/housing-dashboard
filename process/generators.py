# Lint as: python3
"""Contains data and name-value generators for processing schema-less CSV.
"""

from process.types import Field
from process.types import NameValue


def gen_id(proj):
    return [Field('id', proj.id, True)]


def gen_facts(proj):
    result = [Field()] * 5

    if proj.field('address') != '':
        result[0] = Field('address', proj.field('address'))
        result[1] = Field('applicant', '')
        result[2] = Field('supervisor_district', '')
        result[3] = Field('permit_authority', 'planning')
        result[4] = Field('permit_authority_id', proj.field('fk'))

    return result


def gen_units(proj):
    result = [Field()] * 4

    # TODO: how to handle cases where better numbers exist from dbi
    # TODO: how to handle cases where prop - existing != net ?
    result[0] = Field('net_num_units', proj.field('market_rate_units_net'))
    result[1] = Field('net_num_units_data',
                      'planning' if result[0].value != '' else '',
                      True)

    result[2] = Field('net_num_units_bmr', proj.field('affordable_units_net'))
    result[3] = Field('net_num_units_bmr_data',
                      'planning' if result[2].value != '' else '',
                      True)

    return result


def nv_geom(proj):
    if proj.field('the_geom') != '':
        return [NameValue('geom', proj.field('the_geom'), 'planning')]

    return []


def nv_all_units(proj):
    # TODO: more useful when we have more than just one data source
    result = []
    if proj.field('market_rate_units_net'):
        result.append(NameValue('net_num_units',
                                proj.field('market_rate_units_net'),
                                'planning'))
    if proj.field('affordable_units_net'):
        result.append(NameValue('net_num_units_bmr',
                                proj.field('affordable_units_net'),
                                'planning'))
    return result


def nv_square_feet(proj):
    if proj.field('residential_sq_ft_net') != '':
        return [NameValue('net_num_square_feet',
                          proj.field('residential_sq_ft_net'),
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
