# Lint as: python3
"""Contains data and name-value generators for processing schema-less CSV.
"""
import re

from collections import namedtuple

from schemaless.sources import PPTS
from schemaless.sources import PTS

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
        result[3] = Field('permit_authority', PPTS.OUTPUT_NAME)
        result[4] = Field('permit_authority_id', proj.field('fk', PPTS.NAME))

    return result


def _get_dbi_units(proj):
    """
    Returns:
      Net new units from DBI, only if it could be sourced from a new
      construction permit.  None if no data from DBI.
    """
    dbi_exist = 0
    dbi_prop = 0
    try:
        dbi_exist = int(proj.field(
            'existing_units', PTS.NAME,
            entry_predicate=[('permit_type',
                              lambda x: x == '1' or x == '2')]))
    except ValueError:
        pass

    try:
        dbi_prop = int(proj.field(
            'proposed_units', PTS.NAME,
            entry_predicate=[('permit_type',
                              lambda x: x == '1' or x == '2')]))
    except ValueError:
        pass

    if dbi_prop:
        return dbi_prop - dbi_exist

    return None


def gen_units(proj):
    result = [Field()] * 4

    dbi_net = _get_dbi_units(proj)

    if dbi_net is not None:
        result[0] = Field('net_num_units', str(dbi_net))
        result[1] = Field('net_num_units_data', PTS.OUTPUT_NAME, True)
    else:
        # TODO: how to handle cases where prop - existing != net ?
        result[0] = Field('net_num_units',
                          proj.field('market_rate_units_net', PPTS.NAME))
        result[1] = Field('net_num_units_data',
                          PPTS.OUTPUT_NAME if result[0].value else '',
                          True)

    result[2] = Field('net_num_units_bmr',
                      proj.field('affordable_units_net', PPTS.NAME))
    result[3] = Field('net_num_units_bmr_data',
                      PPTS.OUTPUT_NAME if result[2].value else '',
                      True)

    return result


def nv_geom(proj):
    if proj.field('the_geom', PPTS.NAME) != '':
        return [OutputNameValue('geom',
                                proj.field('the_geom', PPTS.NAME),
                                PPTS.OUTPUT_NAME)]

    return []


def nv_all_units(proj):
    result = []
    if proj.field('market_rate_units_net', PPTS.NAME):
        result.append(OutputNameValue(
            'net_num_units',
            proj.field('market_rate_units_net', PPTS.NAME),
            PPTS.OUTPUT_NAME))
    if proj.field('affordable_units_net', PPTS.NAME):
        result.append(OutputNameValue(
            'net_num_units_bmr',
            proj.field('affordable_units_net', PPTS.NAME),
            PPTS.OUTPUT_NAME))

    dbi_net = _get_dbi_units(proj)
    if dbi_net is not None:
        result.append(OutputNameValue('net_num_units',
                                      str(dbi_net),
                                      PTS.OUTPUT_NAME))

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
            result.append(OutputNameValue(field, net, PPTS.OUTPUT_NAME))

    result.append(OutputNameValue('is_adu',
                                  'TRUE' if is_adu else 'FALSE',
                                  PPTS.OUTPUT_NAME))

    return result


def nv_square_feet(proj):
    if proj.field('residential_sq_ft_net', PPTS.NAME) != '':
        return [OutputNameValue('net_num_square_feet',
                                proj.field('residential_sq_ft_net', PPTS.NAME),
                                PPTS.OUTPUT_NAME)]
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
