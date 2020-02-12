# Lint as: python3
"""Classes and subclasses that define our relational tables."""

from abc import ABC
from abc import abstractmethod
from datetime import date
from datetime import datetime

import re

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


class ProjectStatusHistory(Table):
    _PPTS_ENT_CODES = {'ENV', 'AHB', 'COA', 'CUA', 'CTZ', 'DNX', 'ENX',
                       'OFA', 'PTA', 'SHD', 'TDM', 'VAR', 'WLS'}

    TOP_LEVEL_STATUS = 'top_level_status'
    START_DATE = 'start_date'
    END_DATE = 'end_date'
    DATA_SOURCE = 'data_source'

    def __init__(self):
        super().__init__('project_status_history', header=[
            self.TOP_LEVEL_STATUS,
            self.START_DATE,
            self.END_DATE,
            self.DATA_SOURCE])

    def _predevelopment_date(self, proj):
        # TODO: Use the PPA submitted date once we have pulled in the new
        # PPTS data pipeline (if that doesn't exist fall back to using our
        # own logic)
        ppa_opened_field = proj.field(
            'date_opened', PPTS.NAME,
            entry_predicate=[('record_type_category',
                              lambda x: x == 'PPA')])
        if ppa_opened_field:
            ppa_opened_date = datetime.strptime(
                ppa_opened_field.split(' ')[0],
                "%m/%d/%Y").date()
            return (ppa_opened_date.isoformat(), PPTS.OUTPUT_NAME)

        return ('', None)

    def _filed_for_entitlements_date(self, proj):
        # TODO: Use the Application Submitted date once we have pulled
        # in the new PPTS data pipeline (if that doesn't exist fall back to
        # our own logic)

        # Look for the earliest date_opened on an ENT child of a PRJ.
        root = proj.roots[PPTS.NAME]
        if root is None:
            print("Error: Project with non-PPTS root id %s" % proj.id)
            return ('', None)
        if root[0].get_latest('record_type_category')[0] == 'PRJ':
            oldest_open = date.max
            for child in proj.children[PPTS.NAME]:
                record_type = child.get_latest('record_type_category')[0]
                if record_type not in self._PPTS_ENT_CODES:
                    continue

                date_opened_field = child.get_latest('date_opened')[0]
                date_opened = datetime.strptime(
                    date_opened_field.split(' ')[0],
                    "%m/%d/%Y").date()
                if date_opened < oldest_open:
                    oldest_open = date_opened

            if oldest_open < date.max:
                return (oldest_open.isoformat(), PPTS.OUTPUT_NAME)

        return ('', None)

    def _entitled_date(self, proj):
        # TODO: Use the Entitlements Approved date once we have pulled
        # in the new PPTS data pipeline (if that doesn't exist fall back)

        # Look for the ENT child of a PRJ with the latest date_closed
        # (assuming all are closed). Fall back to the PRJ date.
        root = proj.roots[PPTS.NAME]
        if root is None:
            print("Error: Project with non-PPTS root id %s" % proj.id)
            return ('', None)
        if root[0].get_latest('record_type_category')[0] == 'PRJ':
            newest_closed = date.min
            count_closed_no_date = 0
            for child in proj.children[PPTS.NAME]:
                record_type = child.get_latest('record_type_category')[0]
                if record_type not in self._PPTS_ENT_CODES:
                    continue

                date_closed_value = child.get_latest('date_closed')
                status_value = child.get_latest('status')
                if date_closed_value:
                    date_closed = datetime.strptime(
                        date_closed_value[0].split(' ')[0],
                        "%m/%d/%Y").date()
                    if date_closed > newest_closed:
                        newest_closed = date_closed
                elif status_value and 'closed' in status_value[0].lower():
                    count_closed_no_date += 1
                else:
                    # ENT record is not closed, entitlements not approved
                    return ('', None)

            if newest_closed > date.min:
                return (newest_closed.isoformat(), PPTS.OUTPUT_NAME)
            elif count_closed_no_date > 0:
                # Fall back to PRJ date if all ENT child records are closed
                # but there's no date
                date_closed_field = root[0].get_latest('date_closed')[0]
                if date_closed_field:
                    date_closed = datetime.strptime(
                        date_closed_field.split(' ')[0],
                        "%m/%d/%Y").date()
                    return (date_closed.isoformat(), PPTS.OUTPUT_NAME)

        return ('', None)

    def status_row(self,
                   proj,
                   top_level_status='',
                   start_date='',
                   end_date='',
                   data=''):
        row = [''] * len(self.header())
        self.gen_id(row, proj)
        row[self.index(self.TOP_LEVEL_STATUS)] = top_level_status
        row[self.index(self.START_DATE)] = start_date
        row[self.index(self.END_DATE)] = end_date
        row[self.index(self.DATA_SOURCE)] = data
        return row

    def rows(self, proj):
        (predev_date, predev_data) = self._predevelopment_date(proj)
        (filed_date, filed_data) = self._filed_for_entitlements_date(proj)
        (entitled_date, entitled_data) = self._entitled_date(proj)

        result = []
        if predev_date:
            result.append(
                self.status_row(proj,
                                'pre-development',
                                predev_date,
                                filed_date,
                                predev_data))

        if filed_date:
            result.append(
                self.status_row(proj,
                                'filed_for_entitlements',
                                filed_date,
                                entitled_date,
                                filed_data))

        # TODO Add the correct end dates once PTS statuses are added in
        if entitled_date:
            result.append(
                self.status_row(proj,
                                'entitled',
                                entitled_date,
                                '',
                                entitled_data))

        return result
