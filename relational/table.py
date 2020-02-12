# Lint as: python3
"""Classes and subclasses that define our relational tables."""

from abc import ABC
from abc import abstractmethod

import re

from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import PPTS
from schemaless.sources import PTS


class Table(ABC):
    ID = 'id'

    def __init__(self, name, header=[]):
        self.name = name
        self._header = [self.ID] + header

        self._field_position = {}
        for (i, field) in enumerate(self._header):
            self._field_position[field] = i

    def index(self, field):
        return self._field_position[field]

    def header(self):
        return self._header

    def gen_id(self, row, proj):
        row[self.index(self.ID)] = proj.id

    @abstractmethod
    def rows(self, proj):
        pass


class NameValueTable(Table):
    NAME = 'name'
    VALUE = 'value'
    DATA = 'data_source'

    def __init__(self, name):
        super().__init__(name, [self.NAME, self.VALUE, self.DATA])

    def nv_row(self, proj, name='', value='', data=''):
        row = [''] * len(self.header())
        self.gen_id(row, proj)
        row[self.index(self.NAME)] = name
        row[self.index(self.VALUE)] = value
        row[self.index(self.DATA)] = data
        return row


def _get_mohcd_units(proj, source_override=False):
    """
    Gets net new units and bmr counts from the mohcd dataset.  Prioritizes
    data from MOHCDPipeline, and falls back to MOHCDInclusionary if none
    found.

    If source_override is specified, will pull numbers only from the given
    source.  Otherwise will pull from pipeline and inclusionary mohcd data.
    Nothing technically prevents you from providing a non-mohcd source, but
    you will probably not get useful numbers.

    Returns:
      A tuple of (number units, number of BMR units, source) from MOHCD, or
      None if nothing found.
    """
    sources = [source_override] if source_override else [
        MOHCDPipeline.NAME,
        MOHCDInclusionary.NAME]
    net = bmr = None
    for source in sources:
        atleast_one = False
        try:
            net = int(proj.field('total_project_units', source))
            bmr = 0
            atleast_one = True
        except ValueError:
            pass

        try:
            bmr = int(proj.field('total_affordable_units', source))
            if not net:
                net = 0
            atleast_one = True
        except ValueError:
            pass

        if atleast_one:
            break

    return (net, bmr, source) if atleast_one else None


_valid_dbi_permit_types = set('123')


_is_valid_dbi_type = ('permit_type',
                      lambda x: x in _valid_dbi_permit_types)


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
            entry_predicate=[_is_valid_dbi_type]))
    except ValueError:
        pass

    try:
        dbi_prop = int(proj.field(
            'proposed_units', PTS.NAME,
            entry_predicate=[_is_valid_dbi_type]))
    except ValueError:
        pass

    if dbi_prop:
        return dbi_prop - dbi_exist

    return None


class ProjectFacts(Table):
    ADDRESS = 'address'
    APPLICANT = 'applicant'
    SUPERVISOR_DISTRICT = 'supervisor_district'
    PERMIT_AUTHORITY = 'permit_authority'
    PERMIT_AUTHORITY_ID = 'permit_authority_id'
    NET_NUM_UNITS = 'net_num_units'
    NET_NUM_UNITS_DATA = 'net_num_units_data'
    NET_NUM_UNITS_BMR = 'net_num_units_bmr'
    NET_NUM_UNITS_BMR_DATA = 'net_num_units_bmr_data'

    SEEN_IDS = set()

    def __init__(self):
        super().__init__('project_facts', header=[
            self.ADDRESS,
            self.APPLICANT,
            self.SUPERVISOR_DISTRICT,
            self.PERMIT_AUTHORITY,
            self.PERMIT_AUTHORITY_ID,
            self.NET_NUM_UNITS,
            self.NET_NUM_UNITS_DATA,
            self.NET_NUM_UNITS_BMR,
            self.NET_NUM_UNITS_BMR_DATA,
        ])

    def _gen_facts(self, row, proj):
        if proj.field('address', PPTS.NAME) != '':
            row[self.index(self.ADDRESS)] = proj.field('address', PPTS.NAME)
            row[self.index(self.APPLICANT)] = ''  # TODO
            row[self.index(self.SUPERVISOR_DISTRICT)] = ''  # TODO
            row[self.index(self.PERMIT_AUTHORITY)] = PPTS.OUTPUT_NAME
            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.field(
                'fk', PPTS.NAME)

    def _gen_units(self, row, proj):
        mohcd = _get_mohcd_units(proj)
        if mohcd is not None:
            net, bmr, source = mohcd
            row[self.index(self.NET_NUM_UNITS)] = str(net)
            row[self.index(self.NET_NUM_UNITS_DATA)] = source
            row[self.index(self.NET_NUM_UNITS_BMR)] = str(bmr)
            row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = source
        else:
            dbi_net = _get_dbi_units(proj)
            if dbi_net is not None:
                row[self.index(self.NET_NUM_UNITS)] = str(dbi_net)
                row[self.index(self.NET_NUM_UNITS_DATA)] = PTS.OUTPUT_NAME
            else:
                # TODO: how to handle cases where prop - existing != net ?
                net = proj.field('market_rate_units_net', PPTS.NAME)
                row[self.index(self.NET_NUM_UNITS)] = net
                row[self.index(self.NET_NUM_UNITS_DATA)] = \
                    PPTS.OUTPUT_NAME if net else ''

            bmr_net = proj.field('affordable_units_net', PPTS.NAME)
            row[self.index(self.NET_NUM_UNITS_BMR)] = bmr_net
            row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = \
                PPTS.OUTPUT_NAME if bmr_net else ''

    def _atleast_one_measure(self, row):
        return (row[self.index(self.NET_NUM_UNITS)] != '' or
                row[self.index(self.NET_NUM_UNITS_BMR)] != '')

    def rows(self, proj):
        row = [''] * len(self.header())

        self.gen_id(row, proj)
        self._gen_facts(row, proj)
        self._gen_units(row, proj)

        if self._atleast_one_measure(row):
            self.SEEN_IDS.add(row[self.index(self.ID)])
            return [row]

        return []


class ProjectGeo(NameValueTable):
    def __init__(self):
        super().__init__('project_geo')

    def _geom(self, rows, proj):
        geom = proj.field('the_geom', PPTS.NAME)
        if geom != '':
            rows.append(self.nv_row(proj,
                                    name='geom',
                                    value=geom,
                                    data=PPTS.OUTPUT_NAME))

    def rows(self, proj):
        result = []
        self._geom(result, proj)
        return result


class ProjectUnitCountsFull(NameValueTable):
    def __init__(self):
        super().__init__('project_unit_counts_full')

    def _all_units(self, rows, proj):
        ppts_units = proj.field('market_rate_units_net', PPTS.NAME)
        if ppts_units:
            rows.append(self.nv_row(proj,
                                    name='net_num_units',
                                    value=ppts_units,
                                    data=PPTS.OUTPUT_NAME))
        ppts_bmr = proj.field('affordable_units_net', PPTS.NAME)
        if ppts_bmr:
            rows.append(self.nv_row(proj,
                                    name='net_num_units_bmr',
                                    value=ppts_bmr,
                                    data=PPTS.OUTPUT_NAME))

        dbi_net = _get_dbi_units(proj)
        if dbi_net is not None:
            rows.append(self.nv_row(proj,
                                    name='net_num_units',
                                    value=str(dbi_net),
                                    data=PTS.OUTPUT_NAME))

        for source_override in [MOHCDPipeline.NAME, MOHCDInclusionary.NAME]:
            mohcd = _get_mohcd_units(proj, source_override=source_override)
            if mohcd is not None:
                net, bmr, source = mohcd
                rows.append(self.nv_row(proj,
                                        name='net_num_units',
                                        value=str(net),
                                        data=source))
                rows.append(self.nv_row(proj,
                                        name='net_num_units_bmr',
                                        value=str(bmr),
                                        data=source))

    def rows(self, proj):
        result = []
        self._all_units(result, proj)
        return result


class ProjectDetails(NameValueTable):
    def __init__(self):
        super().__init__('project_details')

    def _bedroom_info(self, rows, proj):
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
                rows.append(self.nv_row(proj,
                                        name=field,
                                        value=net,
                                        data=PPTS.OUTPUT_NAME))

        if len(rows) > 0:
            rows.append(self.nv_row(proj,
                                    name='is_adu',
                                    value='TRUE' if is_adu else 'FALSE',
                                    data=PPTS.OUTPUT_NAME))

    def _square_feet(self, rows, proj):
        sqft = proj.field('residential_sq_ft_net', PPTS.NAME)
        if sqft != '':
            rows.append(self.nv_row(proj,
                                    name='net_num_square_feet',
                                    value=sqft,
                                    data=PPTS.OUTPUT_NAME))

    def rows(self, proj):
        result = []
        self._square_feet(result, proj)
        self._bedroom_info(result, proj)
        return result
