# Lint as: python3
"""Classes and subclasses that define our relational tables."""

from abc import ABC
from abc import abstractmethod
from datetime import date
from datetime import datetime
from collections import OrderedDict

import math
import re

from schemaless.sources import AffordableRentalPortfolio
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


# Using an OrderedDict here instead of dict() [which is ordered in py3.7+]
# to be explicit that we care about ordering here.  We are using this
# to approximate an ordered set.
_MOHCD_TYPES = OrderedDict([
    (MOHCDPipeline.NAME, MOHCDPipeline.OUTPUT_NAME),
    (MOHCDInclusionary.NAME, MOHCDInclusionary.OUTPUT_NAME),
    (AffordableRentalPortfolio.NAME, AffordableRentalPortfolio.OUTPUT_NAME),
])


def _get_mohcd_units(proj, source_override=None):
    """
    Gets net new units and bmr counts from the mohcd dataset.  Prioritizes
    data from MOHCDPipeline, and falls back to MOHCDInclusionary if none
    found.

    If source_override is specified, will pull numbers only from the given
    source.  Otherwise will pull from pipeline and inclusionary mohcd data.

    Returns:
      A tuple of (number units, number of BMR units, source) from MOHCD, or
      None if nothing found.

    Raises ValueError if a non-MOHCD source_override was provided.
    """
    if source_override and source_override not in _MOHCD_TYPES:
        raise ValueError('Unknown source_override %s' % source_override)

    sources = [source_override] if source_override else _MOHCD_TYPES.keys()
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

_invalid_dbi_statuses = set(['cancelled', 'withdrawn'])


_is_valid_dbi_entry = [('permit_type',
                        lambda x: x in _valid_dbi_permit_types),
                       ('current_status',
                        lambda x: x == '' or x not in _invalid_dbi_statuses)]


def _get_dbi_units(proj):
    """
    Returns:
      Net new units from DBI, only if it could be sourced from a new
      construction permit or addition.  None if no data from DBI.
    """
    dbi_exist = 0
    dbi_prop = 0
    try:
        fk_entries = proj.fields('existing_units',
                                 PTS.NAME,
                                 entry_predicate=_is_valid_dbi_entry)
        for (fk, entries) in fk_entries.items():
            latest = (None, datetime.min)
            # If we have multiple entries for the same foreign key,
            # de-dupe by selecting the most recent one.
            for entry in entries:
                entry_latest = entry.get_latest('existing_units')
                if entry_latest[1] > latest[1]:
                    latest = entry_latest

            if latest[0]:
                dbi_exist += int(latest[0])
    except ValueError:
        dbi_exist = 0
        pass

    try:
        fk_entries = proj.fields('proposed_units',
                                 PTS.NAME,
                                 entry_predicate=_is_valid_dbi_entry)
        for (fk, entries) in fk_entries.items():
            latest = (None, datetime.min)
            # If we have multiple entries for the same foreign key,
            # de-dupe by selecting the most recent one.
            for entry in entries:
                entry_latest = entry.get_latest('proposed_units')
                if entry_latest[1] > latest[1]:
                    latest = entry_latest

            if latest[0]:
                dbi_prop += int(latest[0])
    except ValueError:
        dbi_prop = 0
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
    NET_EST_NUM_UNITS_BMR = 'net_estimated_num_units_bmr'
    NET_EST_NUM_UNITS_BMR_DATA = 'net_estimated_num_units_bmr_data'

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
            self.NET_EST_NUM_UNITS_BMR,
            self.NET_EST_NUM_UNITS_BMR_DATA,
        ])

    def _gen_facts(self, row, proj):
        pts_pred = [('permit_type', lambda x: x == '1' or x == '2')]
        if proj.field('address', PPTS.NAME) != '':
            row[self.index(self.ADDRESS)] = proj.field('address', PPTS.NAME)
            row[self.index(self.APPLICANT)] = ''  # TODO
            row[self.index(self.SUPERVISOR_DISTRICT)] = ''  # TODO
            row[self.index(self.PERMIT_AUTHORITY)] = PPTS.OUTPUT_NAME
            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.field(
                'fk', PPTS.NAME)
        elif proj.field('permit_number',
                        PTS.NAME,
                        entry_predicate=pts_pred) != '':
            row[self.index(self.ADDRESS)] = '%s %s, %s' % (
                    proj.field('street_number',
                               PTS.NAME,
                               entry_predicate=pts_pred),
                    proj.field('street_name',
                               PTS.NAME,
                               entry_predicate=pts_pred),
                    proj.field('zip_code',
                               PTS.NAME,
                               entry_predicate=pts_pred))
            row[self.index(self.APPLICANT)] = ''  # TODO
            row[self.index(self.SUPERVISOR_DISTRICT)] = \
                proj.field('supervisor_district',
                           PTS.NAME,
                           entry_predicate=pts_pred)
            row[self.index(self.PERMIT_AUTHORITY)] = PTS.NAME
            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.field(
                'fk', PTS.NAME, entry_predicate=pts_pred)
        elif proj.field('project_id', MOHCDPipeline.NAME) != '':
            num = proj.field('street_number', MOHCDPipeline.NAME)
            addr = proj.field('street_name', MOHCDPipeline.NAME)
            if num:
                addr = ('%s %s' % (num, addr))

            row[self.index(self.ADDRESS)] = '%s %s, %s' % (
                    addr,
                    proj.field('street_type', MOHCDPipeline.NAME),
                    proj.field('zip_code', MOHCDPipeline.NAME))
            row[self.index(self.APPLICANT)] = \
                proj.field('project_lead_sponsor', MOHCDPipeline.NAME)
            row[self.index(self.SUPERVISOR_DISTRICT)] = \
                proj.field('supervisor_district', MOHCDPipeline.NAME)
            row[self.index(self.PERMIT_AUTHORITY)] = MOHCDPipeline.NAME
            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.field(
                'fk', MOHCDPipeline.NAME)
        elif proj.field('project_id', MOHCDInclusionary.NAME) != '':
            num = proj.field('street_number', MOHCDInclusionary.NAME)
            addr = proj.field('street_name', MOHCDInclusionary.NAME)
            if num:
                addr = ('%s %s' % (num, addr))

            row[self.index(self.ADDRESS)] = '%s %s, %s' % (
                    addr,
                    proj.field('street_type', MOHCDInclusionary.NAME),
                    proj.field('zip_code', MOHCDInclusionary.NAME))
            row[self.index(self.APPLICANT)] = \
                proj.field('project_lead_sponsor', MOHCDInclusionary.NAME)
            row[self.index(self.SUPERVISOR_DISTRICT)] = \
                proj.field('supervisor_district', MOHCDInclusionary.NAME)
            row[self.index(self.PERMIT_AUTHORITY)] = MOHCDInclusionary.NAME
            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.field(
                'fk', MOHCDInclusionary.NAME)

    def _estimate_bmr(self, net):
        """Estimates the BMR we project a project to have.

        This exists because currently all/most projects in ppts have nothing
        specified for their affordable unit counts, but we can provide
        a rough estimate of what we expect the project to have when it gets
        entitled.

        net: a (string) net unit count

        Returns: a non-empty string
        """
        # TODO: this logic can get pretty complicated if needed, but right
        # now this is just a basic "floor" number that leans on the side of
        # undercounting.
        net = int(net)
        if net < 10:
            return '0'
        else:
            # based on the inclusionary affordable housing program as of 2019
            if net < 25:
                return str(math.floor(.2 * net))
            else:
                return str(math.floor(.3 * net))

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
            net = dbi_net
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
            if bmr_net != '':
                row[self.index(self.NET_NUM_UNITS_BMR)] = bmr_net
                row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = PPTS.OUTPUT_NAME
            elif net != '':
                row[self.index(self.NET_EST_NUM_UNITS_BMR)] = \
                    self._estimate_bmr(net)
                row[self.index(self.NET_EST_NUM_UNITS_BMR_DATA)] = \
                    PPTS.OUTPUT_NAME

    def _atleast_one_measure(self, row):
        return (row[self.index(self.NET_NUM_UNITS)] != '' or
                row[self.index(self.NET_NUM_UNITS_BMR)] != '' or
                row[self.index(self.NET_EST_NUM_UNITS_BMR)] != '')

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
    # NOTE/TODO: expand as needed
    OUT_1BR = 'residential_units_1br'
    OUT_2BR = 'residential_units_2br'
    OUT_3BR = 'residential_units_3br'
    OUT_4BR = 'residential_units_4br'

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
                      self.OUT_1BR,
                      self.OUT_2BR,
                      self.OUT_3BR,
                      # No OUT_4BR because no 4br data in PPTS
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

    _MOHCD_BEDROOM_MAP = {
        'num_1bd_units': OUT_1BR,
        'num_2bd_units': OUT_2BR,
        'num_3bd_units': OUT_3BR,
        'num_4bd_units': OUT_4BR,
    }

    def _get_mohcd_fields(self, proj, fieldmap):
        """Extracts information from MOHCD, preferring Pipeline over
        Inclusionary.

        fieldmap: a dict of the mohcd field name to an output field name
        to use in the returned tuple.

        Returns:
            A list of (name, value, source) tuples.  If there was not
            at least one non-zero value, then the list will be empty,
            regardless of whether the field existed in the MOHCD source.
        """
        out = []
        nonzero = False
        for (source, outsource) in _MOHCD_TYPES.items():
            added = False
            for (mohcdfield, outfield) in fieldmap.items():
                try:
                    rawnet = int(proj.field(mohcdfield, source))
                    nonzero = nonzero or rawnet != 0
                    net = str(rawnet)

                    out.append((outfield, net, outsource))
                    added = True
                except ValueError:
                    pass

            if added:
                break

        return out if nonzero else []

    def _bedroom_info_mohcd(self, rows, proj):
        """Populates bedroom information from MOHCD.

        Only pulls data from one MOHCD source, preferring Pipeline over
        Inclusionary.  This is because this is a matter of correctness and
        unnecessary duplication, rather than completeness.
        """
        for datum in self._get_mohcd_fields(proj, self._MOHCD_BEDROOM_MAP):
            rows.append(self.nv_row(proj,
                                    name=datum[0],
                                    value=datum[1],
                                    data=datum[2]))

    # Note that some of these fields are not expected to be in all MOHCD
    # data sets because they have different levels of granularity; any code
    # consuming this should be aware of that.
    _MOHCD_AMI_FIELDS = {
        'num_20_percent_ami_units': 'num_20_percent_ami_units',
        'num_30_percent_ami_units': 'num_30_percent_ami_units',
        'num_40_percent_ami_units': 'num_40_percent_ami_units',
        'num_50_percent_ami_units': 'num_50_percent_ami_units',
        'num_55_percent_ami_units': 'num_55_percent_ami_units',
        'num_60_percent_ami_units': 'num_60_percent_ami_units',
        'num_80_percent_ami_units': 'num_80_percent_ami_units',
        'num_90_percent_ami_units': 'num_90_percent_ami_units',
        'num_100_percent_ami_units': 'num_100_percent_ami_units',
        'num_105_percent_ami_units': 'num_105_percent_ami_units',
        'num_110_percent_ami_units': 'num_110_percent_ami_units',
        'num_120_percent_ami_units': 'num_120_percent_ami_units',
        'num_130_percent_ami_units': 'num_130_percent_ami_units',
        'num_150_percent_ami_units': 'num_150_percent_ami_units',
        'num_ami_undeclared_units': 'num_ami_undeclared_units',
        'num_more_than_120_percent_ami_units':
            'num_more_than_120_percent_ami_units',
    }

    def _ami_info_mohcd(self, rows, proj):
        """Populates AMI information from MOHCD.

        Only pulls data from one MOHCD source, preferring Pipeline over
        Inclusionary.  This is because this is a matter of correctness and
        unnecessary duplication, rather than completeness.
        """
        for datum in self._get_mohcd_fields(proj,
                                            self._MOHCD_AMI_FIELDS):
            rows.append(self.nv_row(proj,
                                    name=datum[0],
                                    value=datum[1],
                                    data=datum[2]))

    _IS_100_AFFORDABLE_FIELDMAP = {
        'total_project_units': 'total_project_units',
        'total_affordable_units': 'total_affordable_units',
    }

    def _is_100_affordable(self, rows, proj):
        """Populates whether a project is 100% affordable, at least insofar
        as we can tell from MOHCD data.
        """
        units = _get_mohcd_units(proj, MOHCDPipeline.NAME)
        if units and units[0] > 0:
            rows.append(self.nv_row(
                proj,
                name='is_100pct_affordable',
                value='TRUE' if units[0] == units[1] else 'FALSE',
                data=MOHCDPipeline.OUTPUT_NAME))
        else:
            units = _get_mohcd_units(proj, AffordableRentalPortfolio.NAME)
            if units and units[0] > 0:
                rows.append(self.nv_row(
                        proj,
                        name='is_100pct_affordable',
                        value='TRUE',
                        data=AffordableRentalPortfolio.OUTPUT_NAME))

    def _square_feet(self, rows, proj):
        sqft = proj.field('residential_sq_ft_net', PPTS.NAME)
        if sqft != '':
            rows.append(self.nv_row(proj,
                                    name='net_num_square_feet',
                                    value=sqft,
                                    data=PPTS.OUTPUT_NAME))

    def _onsite_or_feeout(self, rows, proj):
        for (mohcdin, mohcdout) in _MOHCD_TYPES.items():
            s415 = proj.field('section_415_declaration', mohcdin)

            if s415 != '':
                rows.append(self.nv_row(
                        proj,
                        name='inclusionary_housing_program_status',
                        value=s415,
                        data=mohcdout))
                break

    def rows(self, proj):
        result = []
        self._square_feet(result, proj)
        self._bedroom_info(result, proj)
        self._bedroom_info_mohcd(result, proj)
        self._ami_info_mohcd(result, proj)
        self._is_100_affordable(result, proj)
        self._onsite_or_feeout(result, proj)
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
