# Lint as: python3
"""Classes and subclasses that define our relational tables."""

from abc import ABC
from abc import abstractmethod
from datetime import date
from datetime import datetime
from collections import OrderedDict

import math
import queue
import re

import schemaless.mapblklot_generator as mapblklot_gen
from schemaless.sources import AffordableRentalPortfolio
from schemaless.sources import MOHCDInclusionary
from schemaless.sources import MOHCDPipeline
from schemaless.sources import OEWDPermits
from schemaless.sources import PermitAddendaSummary
from schemaless.sources import Planning
from schemaless.sources import PTS
from schemaless.sources import TCO


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

    def log_bad_data(self):
        pass

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


_is_valid_ocii_project = [('delivery_agency', lambda x: x == 'OCII')]


def _get_ocii_units(proj):
    """
    Gets net new units and bmr counts from the OEWD dataset for housing from
    OCII.

    Returns:
      A tuple of (number units, number of BMR units) from OEWD, or
      None if nothing found.
    """
    net = bmr = None
    atleast_one = False

    # TODO: Once dataset has been changed to not have duplicate count numbers,
    # we should sum up the unit counts of all OEWD children
    try:
        net = int(proj.field('total_units',
                             OEWDPermits.NAME,
                             entry_predicate=_is_valid_ocii_project))
        bmr = 0
        atleast_one = True
    except ValueError:
        pass

    try:
        bmr = int(proj.field('affordable_units',
                             OEWDPermits.NAME,
                             entry_predicate=_is_valid_ocii_project))
        if not net:
            net = 0
        atleast_one = True
    except ValueError:
        pass

    return (net, bmr) if atleast_one else None


_valid_dbi_permit_types = set('123')

_invalid_dbi_statuses = set(['cancelled', 'withdrawn', 'expired'])


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


def _get_tco_units(proj):
    """
    Returns:
      Net new units from TCO dataset (summation number of all unit numbers
      from existing permits). None if no data in TCO.
    """
    num_tco_units = 0
    try:
        fk_entries = proj.fields('num_units', TCO.NAME)
        for (_, entries) in fk_entries.items():
            # Add up all units, even if there are dupe foreign keys
            for entry in entries:
                entry_latest = entry.get_latest('num_units')
                if entry_latest[0]:
                    num_tco_units += int(entry_latest[0])
    except ValueError:
        num_tco_units = 0
        pass

    return num_tco_units if num_tco_units else None


def _get_earliest_date(proj, field, source, predicate, date_fmt):
    """
    Returns:
      The earliest date when there are multiple entries for the same
      field with different dates (and just taking the latest entry may
      not suffice)
    """
    date = datetime.max
    try:
        fk_entries = proj.fields(field, source, entry_predicate=predicate)
        for (_, entries) in fk_entries.items():
            for entry in entries:
                entry_latest = entry.get_latest(field)
                if entry_latest[0]:
                    date_entry = datetime.strptime(entry_latest[0], date_fmt)
                    if date_entry < date:
                        date = date_entry
    except ValueError:
        date = datetime.max
        pass

    return date.date() if date < datetime.max else None


def _get_earliest_addenda_arrival_date(proj):
    """
    Returns:
      The earliest addenda arrival dates for all data associated with PTS
      permits for a single project. None if no data in PTS.
    """
    return _get_earliest_date(proj,
                              'earliest_addenda_arrival',
                              PermitAddendaSummary.NAME,
                              [],
                              "%Y-%m-%d")


class ProjectFacts(Table):
    NAME = 'name'
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
    PIM_LINK = 'pim_link'

    SEEN_IDS = set()

    def __init__(self):
        super().__init__('project_facts', header=[
            self.NAME,
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
            self.PIM_LINK,
        ])

    _ZIP_CODE_REGEX = re.compile(' [0-9]{5}$')

    def _gen_facts(self, row, proj):
        """Generates the basic non-numeric details about a project.

        In terms of departmental data, we only use PTS as a fallback for when
        we don't have data from planning.  However, if we have MOHCD data,
        we use that *even if* we have data from planning.  This is just a
        loose rule in how we rely on these non-numeric details to be most
        accurate.
        """
        used_mohcd = False
        for mohcd in _MOHCD_TYPES.keys():
            if used_mohcd or proj.field('project_id', mohcd) == '':
                continue

            used_mohcd = True

            num = proj.field('street_number', mohcd)
            addr = '%s %s' % (proj.field('street_name', mohcd),
                              proj.field('street_type', mohcd))
            if num:
                addr = ('%s %s' % (num, addr))

            name = proj.field('project_name', mohcd)
            if not name:
                name = addr

            row[self.index(self.NAME)] = name
            row[self.index(self.ADDRESS)] = '%s, %s' % (
                    addr,
                    proj.field('zip_code', mohcd))
            sponsor = proj.field('project_lead_sponsor', mohcd)
            if not sponsor:
                sponsor = proj.field('project_sponsor', mohcd)
            row[self.index(self.APPLICANT)] = sponsor

            row[self.index(self.SUPERVISOR_DISTRICT)] = \
                proj.field('supervisor_district', mohcd)

        if used_mohcd:
            return

        addr = proj.field('address', Planning.NAME)
        name = proj.field('name', Planning.NAME)
        if name or addr:
            if not name:
                name = re.sub(self._ZIP_CODE_REGEX, '', addr)

            row[self.index(self.NAME)] = name
            row[self.index(self.ADDRESS)] = addr
            row[self.index(self.APPLICANT)] = ''  # TODO
            row[self.index(self.SUPERVISOR_DISTRICT)] = ''  # TODO
        elif proj.field('permit_number',
                        PTS.NAME,
                        entry_predicate=_is_valid_dbi_entry) != '':
            street = '%s %s' % (
                proj.field('street_number',
                           PTS.NAME,
                           entry_predicate=_is_valid_dbi_entry),
                proj.field('street_name',
                           PTS.NAME,
                           entry_predicate=_is_valid_dbi_entry))
            addr = '%s, %s' % (
                street,
                proj.field('zip_code',
                           PTS.NAME,
                           entry_predicate=_is_valid_dbi_entry))

            row[self.index(self.NAME)] = street
            row[self.index(self.ADDRESS)] = addr
            row[self.index(self.APPLICANT)] = ''  # TODO
            row[self.index(self.SUPERVISOR_DISTRICT)] = \
                proj.field('supervisor_district',
                           PTS.NAME,
                           entry_predicate=_is_valid_dbi_entry)

    def _estimate_bmr(self, net):
        """Estimates the BMR we project a project to have.

        This exists because currently all/most projects in planning have
        nothing specified for their affordable unit counts, but we can provide
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
        ocii = _get_ocii_units(proj)
        if mohcd is not None:
            net, bmr, source = mohcd
            row[self.index(self.NET_NUM_UNITS)] = str(net)
            row[self.index(self.NET_NUM_UNITS_DATA)] = source
            row[self.index(self.NET_NUM_UNITS_BMR)] = str(bmr)
            row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = source
        elif ocii is not None:
            net, bmr = ocii
            row[self.index(self.NET_NUM_UNITS)] = str(net)
            row[self.index(self.NET_NUM_UNITS_DATA)] = \
                OEWDPermits.OUTPUT_NAME
            row[self.index(self.NET_NUM_UNITS_BMR)] = str(bmr)
            row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = \
                OEWDPermits.OUTPUT_NAME
        else:
            dbi_net = _get_dbi_units(proj)
            planning_net = proj.field('number_of_units', Planning.NAME)
            net = dbi_net
            # PTS may have an explicitly set 0 unit count for projects
            # that have no business dealing with housing (possible with
            # permit type 3 in particular), so we only emit a 0-count
            # PTS unit count if we had an explicit non-0 Planning unit
            # count (therefore indicating a housing-related project that
            # lost its housing somehow).
            if (dbi_net is not None
                    and (dbi_net != 0 or
                         (planning_net and planning_net != '0'))):
                row[self.index(self.NET_NUM_UNITS)] = str(dbi_net)
                row[self.index(self.NET_NUM_UNITS_DATA)] = PTS.OUTPUT_NAME
            else:
                try:
                    # Only fallback to using planning if we have a non-zero
                    # unit count, because we always have a 0 even for
                    # irrelevant projects.
                    net = int(planning_net)
                    if net != 0:
                        row[self.index(self.NET_NUM_UNITS)] = planning_net
                        row[self.index(self.NET_NUM_UNITS_DATA)] = \
                            Planning.OUTPUT_NAME
                    else:
                        net = None
                except ValueError:
                    net = None
                    pass
            bmr_net = proj.field('number_of_affordable_units', Planning.NAME)
            if bmr_net and bmr_net != '0':
                row[self.index(self.NET_NUM_UNITS_BMR)] = bmr_net
                row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = \
                    Planning.OUTPUT_NAME
            elif net is not None:
                row[self.index(self.NET_EST_NUM_UNITS_BMR)] = \
                    self._estimate_bmr(net)
                row[self.index(self.NET_EST_NUM_UNITS_BMR_DATA)] = \
                    Planning.OUTPUT_NAME

    def _pim_link_info(self, row, proj):
        root_type = [('record_type',
                      lambda x: x in _valid_planning_root_type)]
        prj_id = proj.field('record_id',
                            Planning.NAME,
                            entry_predicate=root_type)
        pim_link_template = "https://sfplanninggis.org/pim?search=%s"
        if prj_id:
            row[self.index(self.PIM_LINK)] = pim_link_template % prj_id
        else:
            blocklot = proj.field('mapblocklot', Planning.NAME)
            if blocklot:
                row[self.index(self.PIM_LINK)] = pim_link_template % blocklot
            else:
                block = proj.field('block', PTS.NAME)
                lot = proj.field('lot', PTS.NAME)
                if block and lot:
                    blocklot = block + lot
                    row[self.index(self.PIM_LINK)] = \
                        pim_link_template % blocklot
                else:
                    row[self.index(self.PIM_LINK)] = ''

    def _permit_authority_info(self, row, proj):
        prj_roots = proj.roots[Planning.NAME]
        ocii_proj_name = proj.field('project_name',
                                    OEWDPermits.NAME,
                                    entry_predicate=_is_valid_ocii_project)
        if prj_roots is not None and len(prj_roots) > 0:
            row[self.index(self.PERMIT_AUTHORITY)] = Planning.OUTPUT_NAME

            root_entry = prj_roots[0].get_latest('record_id')
            if root_entry:
                row[self.index(self.PERMIT_AUTHORITY_ID)] = root_entry[0]
        elif ocii_proj_name:
            row[self.index(self.PERMIT_AUTHORITY)] = "ocii"
            row[self.index(self.PERMIT_AUTHORITY_ID)] = ocii_proj_name
        else:
            row[self.index(self.PERMIT_AUTHORITY)] = ''
            row[self.index(self.PERMIT_AUTHORITY_ID)] = ''

    def _atleast_one_measure(self, row):
        return ((row[self.index(self.NET_NUM_UNITS)] != '' and
                 row[self.index(self.NET_NUM_UNITS)] != '0') or
                (row[self.index(self.NET_NUM_UNITS_BMR)] != '' and
                 row[self.index(self.NET_NUM_UNITS_BMR)] != '0') or
                (row[self.index(self.NET_EST_NUM_UNITS_BMR)] != '' and
                 row[self.index(self.NET_EST_NUM_UNITS_BMR)] != '0'))

    def _invalid_prj_root(self, proj):
        invalid_prj_count = 0
        try:
            record_type = [('record_type',
                            lambda x: x in _valid_planning_root_type)]
            fk_entries = proj.fields('status',
                                     Planning.NAME,
                                     entry_predicate=record_type)
            for (_, entries) in fk_entries.items():
                for entry in entries:
                    entry_latest = entry.get_latest('status')
                    if entry_latest:
                        status = entry_latest[0].lower()
                        if all(
                                x not in status
                                for x in _invalid_status_keywords):
                            return False
                        else:
                            invalid_prj_count += 1
        except ValueError:
            return True

        return True if invalid_prj_count > 0 else False

    def _nonzero_or_nonempty_address(self, row):
        """Returns true if this row had a non-empty address, or had an
        empty address but a non-zero net unit count"""
        if row[self.index(self.ADDRESS)] != '':
            return True

        if (row[self.index(self.NET_NUM_UNITS)] and
                row[self.index(self.NET_NUM_UNITS)] != '0'):
            return True
        return False

    def rows(self, proj):
        row = [''] * len(self.header())

        if self._invalid_prj_root(proj):
            return []

        self.gen_id(row, proj)
        self._gen_facts(row, proj)
        self._gen_units(row, proj)
        self._pim_link_info(row, proj)
        self._permit_authority_info(row, proj)

        if (self._atleast_one_measure(row) and
                self._nonzero_or_nonempty_address(row)):
            self.SEEN_IDS.add(row[self.index(self.ID)])
            return [row]

        return []


class ProjectGeo(NameValueTable):
    def __init__(self):
        super().__init__('project_geo')

    def _geom(self, rows, proj):
        # TODO(sbuss): We need this field to be added back
        geom = proj.field('the_geom', Planning.NAME)
        if geom != '':
            rows.append(self.nv_row(proj,
                                    name='geom',
                                    value=geom,
                                    data=Planning.OUTPUT_NAME))

    def _lnglat(self, rows, proj):
        '''Extract an arbitrary longitude and latitude.'''
        blocklot = proj.field('mapblocklot', Planning.NAME)
        if blocklot:
            blkloter = mapblklot_gen.MapblklotGeneratorSingleton.get_instance()
            lnglat = blkloter.find_lnglat_for_blklot(blocklot)
            if lnglat:
                rows.append(self.nv_row(proj,
                                        name='lng',
                                        value=lnglat[0],
                                        data=Planning.OUTPUT_NAME))
                rows.append(self.nv_row(proj,
                                        name='lat',
                                        value=lnglat[1],
                                        data=Planning.OUTPUT_NAME))
        else:
            location = proj.field('location', PTS.NAME)
            if not location:
                return

            lnglat = re.search(r"([0-9.-]+).+?([0-9.-]+)", location)
            if len(lnglat.groups()) != 2:
                return
            rows.append(self.nv_row(proj,
                                    name='lat',
                                    value=lnglat.group(1),
                                    data=PTS.OUTPUT_NAME))
            rows.append(self.nv_row(proj,
                                    name='lng',
                                    value=lnglat.group(2),
                                    data=PTS.OUTPUT_NAME))

    def rows(self, proj):
        result = []
        self._geom(result, proj)
        self._lnglat(result, proj)
        return result


class ProjectUnitCountsFull(NameValueTable):
    def __init__(self):
        super().__init__('project_unit_counts_full')

    def _all_units(self, rows, proj):
        planning_units = proj.field('number_of_units', Planning.NAME)
        if planning_units:
            rows.append(self.nv_row(proj,
                                    name='net_num_units',
                                    value=planning_units,
                                    data=Planning.OUTPUT_NAME))
        planning_bmr = proj.field('number_of_affordable_units', Planning.NAME)
        if planning_bmr:
            rows.append(self.nv_row(proj,
                                    name='net_num_units_bmr',
                                    value=planning_bmr,
                                    data=Planning.OUTPUT_NAME))

        dbi_net = _get_dbi_units(proj)
        if dbi_net is not None:
            rows.append(self.nv_row(proj,
                                    name='net_num_units',
                                    value=str(dbi_net),
                                    data=PTS.OUTPUT_NAME))

        tco_net = _get_tco_units(proj)
        if tco_net is not None:
            rows.append(self.nv_row(proj,
                                    name='net_num_units',
                                    value=str(tco_net),
                                    data=TCO.OUTPUT_NAME))

        ocii = _get_ocii_units(proj)
        if ocii is not None:
            net, bmr = ocii
            rows.append(self.nv_row(proj,
                                    name='net_num_units',
                                    value=str(net),
                                    data=OEWDPermits.OUTPUT_NAME))
            rows.append(self.nv_row(proj,
                                    name='net_num_units_bmr',
                                    value=str(bmr),
                                    data=OEWDPermits.OUTPUT_NAME))

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


class ProjectCompletedUnitCounts(Table):
    NUM_UNITS_COMPLETED = 'num_units_completed'
    DATE_COMPLETED = 'date_completed'
    DATA_SOURCE = 'data_source'

    def __init__(self):
        super().__init__('project_completed_unit_counts', header=[
            self.NUM_UNITS_COMPLETED,
            self.DATE_COMPLETED,
            self.DATA_SOURCE])

    def _completed_units(self, rows, proj):
        """Outputs the records associated with units being completed.
        Prefers to use TCO data if available, but if it's not will look at
        site permits in PTS."""
        for child in proj.children[TCO.NAME]:
            date_issued_field = child.get_latest('date_issued')[0]
            date_issued = datetime.strptime(
                date_issued_field.split(' ')[0],
                '%Y/%m/%d').date()
            num_units = child.get_latest('num_units')[0]

            rows.append(
                self.completed_unit_row(proj,
                                        num_units,
                                        date_issued.isoformat(),
                                        TCO.OUTPUT_NAME))
        # Data exists in TCO data set, don't bother looking in PTS
        if len(rows) > 0:
            return

        seen_permit_numbers = set()
        for child in proj.children[PTS.NAME]:
            permit_number = child.get_latest('permit_number')[0]
            if permit_number in seen_permit_numbers:
                continue
            else:
                seen_permit_numbers.add(permit_number)

            permit_type = child.get_latest('permit_type')[0]
            if permit_type not in _valid_dbi_permit_types:
                continue
            status_entry = child.get_latest('current_status')
            if not status_entry:
                continue
            status = status_entry[0]
            if status != 'complete':
                continue

            date_completed_entry = child.get_latest('completed_date')
            if not date_completed_entry:
                continue
            date_completed_field = date_completed_entry[0]
            date_completed = datetime.strptime(
                date_completed_field.split(' ')[0],
                '%m/%d/%Y').date()
            num_units_prop_entry = child.get_latest('proposed_units')
            if not num_units_prop_entry:
                continue

            num_units_exist = 0
            num_units_exist_entry = child.get_latest('existing_units')
            if num_units_exist_entry:
                num_units_exist = int(num_units_exist_entry[0])
            num_units = int(num_units_prop_entry[0]) - num_units_exist
            rows.append(
                self.completed_unit_row(proj,
                                        str(num_units),
                                        date_completed.isoformat(),
                                        PTS.OUTPUT_NAME))

    def completed_unit_row(self,
                           proj,
                           num_units_completed='',
                           date_completed='',
                           data=''):
        row = [''] * len(self.header())
        self.gen_id(row, proj)
        row[self.index(self.NUM_UNITS_COMPLETED)] = num_units_completed
        row[self.index(self.DATE_COMPLETED)] = date_completed
        row[self.index(self.DATA_SOURCE)] = data
        return row

    def rows(self, proj):
        result = []
        self._completed_units(result, proj)
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
        was_mohcd = False
        for mohcd in _MOHCD_TYPES.keys():
            if proj.field('project_id', mohcd):
                self._bedroom_info_mohcd(rows, proj, mohcd)
                was_mohcd = True
                break

        if not was_mohcd:
            self._bedroom_info_planning(rows, proj)

    def _bedroom_info_planning(self, rows, proj):
        is_adu = False

        def _crunch_number(prefix):
            nonlocal is_adu
            net = 0
            ok = False
            try:
                exist = int(proj.field(prefix + '_exist', Planning.NAME))
                proposed = int(proj.field(prefix + '_prop', Planning.NAME))
                net = str(proposed - exist)
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
                      # No OUT_4BR because no 4br data in Planning
                      'residential_units_micro',
                      'residential_units_sro']:
            (net, ok) = _crunch_number(field)
            if ok:
                rows.append(self.nv_row(proj,
                                        name=field,
                                        value=net,
                                        data=Planning.OUTPUT_NAME))

        if len(rows) > 0:
            rows.append(self.nv_row(proj,
                                    name='is_adu',
                                    value='TRUE' if is_adu else 'FALSE',
                                    data=Planning.OUTPUT_NAME))

    _MOHCD_BEDROOM_MAP = {
        'num_1bd_units': OUT_1BR,
        'num_2bd_units': OUT_2BR,
        'num_3bd_units': OUT_3BR,
        'num_4bd_units': OUT_4BR,
    }

    def _get_mohcd_fields(self, proj, fieldmap, mohcd=None):
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
            if mohcd and source != mohcd:
                continue

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

    def _bedroom_info_mohcd(self, rows, proj, mohcd):
        """Populates bedroom information from MOHCD.

        Only pulls data from one MOHCD source, preferring Pipeline over
        Inclusionary.  This is because this is a matter of correctness and
        unnecessary duplication, rather than completeness.
        """
        for datum in self._get_mohcd_fields(proj,
                                            self._MOHCD_BEDROOM_MAP,
                                            mohcd):
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

    _AFFORDABILITY_THRESHOLD = .9

    def _is_100_affordable(self, rows, proj):
        """Populates whether a project is 100% affordable, at least insofar
        as we can tell from MOHCD data or OEWD data.
        """
        units = _get_mohcd_units(proj, MOHCDPipeline.NAME)
        if units and units[0] > 0:
            rows.append(self.nv_row(
                proj,
                name='is_100pct_affordable',
                value='TRUE'
                      if units[0] * self._AFFORDABILITY_THRESHOLD <= units[1]
                      else 'FALSE',
                data=MOHCDPipeline.OUTPUT_NAME))
        else:
            units = _get_mohcd_units(proj, AffordableRentalPortfolio.NAME)
            if units and units[0] > 0:
                rows.append(self.nv_row(
                        proj,
                        name='is_100pct_affordable',
                        value='TRUE',
                        data=AffordableRentalPortfolio.OUTPUT_NAME))
            else:
                units = _get_ocii_units(proj)
                if units and units[0] > 0:
                    threshold_affordable = \
                        units[0] * self._AFFORDABILITY_THRESHOLD <= units[1]
                    rows.append(self.nv_row(
                        proj,
                        name='is_100pct_affordable',
                        value='TRUE' if threshold_affordable else 'FALSE',
                        data=OEWDPermits.OUTPUT_NAME))

    def _square_feet(self, rows, proj):
        # TODO: This field is gone
        sqft = proj.field('residential_sq_ft_net', Planning.NAME)
        if sqft != '':
            rows.append(self.nv_row(proj,
                                    name='net_num_square_feet',
                                    value=sqft,
                                    data=Planning.OUTPUT_NAME))

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

    def _earliest_addenda_arrival(self, rows, proj):
        date = _get_earliest_addenda_arrival_date(proj)
        if date:
            rows.append(self.nv_row(proj,
                                    name='earliest_addenda_arrival',
                                    value=date.isoformat(),
                                    data=PermitAddendaSummary.OUTPUT_NAME))

    def _env_review_type(self, rows, proj):
        env_review_type = proj.field('environmental_review_type',
                                     Planning.NAME)
        if env_review_type:
            rows.append(self.nv_row(proj,
                                    name='environmental_review_type',
                                    value=env_review_type,
                                    data=Planning.OUTPUT_NAME))

            bucketed = 'Other'
            if re.search('categorical exemption', env_review_type, re.I):
                bucketed = 'Categorical Exemption'
            elif re.search('community plan', env_review_type, re.I):
                bucketed = 'Community Plan'
            elif re.search(r'environmental impact repo|\beir\b',
                           env_review_type,
                           re.I):
                bucketed = 'EIR'
            elif re.search('negative declaration', env_review_type, re.I):
                bucketed = 'Negative Declaration'
            rows.append(self.nv_row(proj,
                                    name='environmental_review_type_bucketed',
                                    value=bucketed,
                                    data=Planning.OUTPUT_NAME))

    def _is_da_type(self, rows, proj):
        """Populates whether a project is a DA or not. Relies on the existence
        of PHA permits (if it is a Planning project) or it existing in the
        OEWD permits data set.
        """
        pha_record_id = proj.field('record_id',
                                   Planning.NAME,
                                   entry_predicate=[('record_type',
                                                     lambda x: x == 'PHA')])
        oewd_record_id = proj.field('row_number',
                                    OEWDPermits.NAME,
                                    entry_predicate=_is_valid_ocii_project)

        is_da = pha_record_id or oewd_record_id
        is_da_data = OEWDPermits.OUTPUT_NAME \
            if oewd_record_id else Planning.OUTPUT_NAME
        rows.append(self.nv_row(proj,
                                name='is_da',
                                value='TRUE' if is_da else 'FALSE',
                                data=is_da_data))

    def _rehab_info(self, rows, proj):
        project_type = proj.field('project_type', MOHCDPipeline.NAME)
        if project_type:
            rows.append(self.nv_row(
                proj,
                name='is_rehab',
                value='TRUE'
                      if project_type.lower() == 'rehabilitation'
                      else 'FALSE',
                data=MOHCDPipeline.OUTPUT_NAME))

    def _incentives_info(self, rows, proj):
        incentives = {'sb35', 'sb330', 'ab2162', 'homesf',
                      'housing_sustainability_dist'}

        for incentive in incentives:
            incentive_field = proj.field(incentive, Planning.NAME)
            if incentive_field:
                rows.append(self.nv_row(
                    proj,
                    name=incentive,
                    value='TRUE'
                          if incentive_field.lower() == 'checked' else 'FALSE',
                    data=Planning.OUTPUT_NAME))

        state_density_bonus = \
            proj.field('state_density_bonus_individual', Planning.NAME)
        if state_density_bonus:
            rows.append(self.nv_row(
                proj,
                name='state_density_bonus',
                value='TRUE'
                      if state_density_bonus.lower() == 'checked' else 'FALSE',
                data=Planning.OUTPUT_NAME))

    def _unique(self, rows):
        """Prunes duplicate name-value entries, preferring entries that were
        added later in the process.
        """
        seen = set()

        def _is_already_seen(row):
            nonlocal seen

            name = row[self.index(self.NAME)]
            if name in seen:
                return True
            seen.add(name)
            return False

        rows[:] = [row for row in reversed(rows) if not _is_already_seen(row)]

    def rows(self, proj):
        """Generates all the rows for this project.

        As a name-value table we nonetheless expect this table to be pivoted,
        so the names are for each project."""
        result = []

        # Order here matters, because _unique will prune earlier entries
        # in favor of identical names added later.
        self._square_feet(result, proj)
        self._bedroom_info(result, proj)
        self._ami_info_mohcd(result, proj)
        self._is_100_affordable(result, proj)
        self._onsite_or_feeout(result, proj)
        self._earliest_addenda_arrival(result, proj)
        self._env_review_type(result, proj)
        self._is_da_type(result, proj)
        self._rehab_info(result, proj)
        self._incentives_info(result, proj)

        self._unique(result)

        return result


_valid_planning_ent_codes = set(['AHB', 'COA', 'CUA', 'CTZ', 'DNX',
                                 'ENX', 'OFA', 'PTA', 'SHD', 'TDM', 'VAR',
                                 'WLS', 'ENV'])
_valid_planning_root_type = set(['PRJ', 'PRL'])
_invalid_status_keywords = set(['cancelled', 'withdrawn', 'disapproved',
                                'removed'])


class ProjectStatusHistory(Table):
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
        self.non_sqntl_dates = 0
        self.non_sqntl_dates_sample = {}
        self.non_consecutive_status = 0
        self.non_consecutive_status_sample = {}

    def _under_entitlement_review_date(self, proj):
        """Look for the earliest of the Application Submitted and Application
        Accepted dates if they exist. If not, look for the earliest open
        ENT record.
        """
        date_submitted_entry = proj.field('date_application_submitted',
                                          Planning.NAME)
        date_submitted = None
        if date_submitted_entry:
            date_submitted = datetime.strptime(
                date_submitted_entry.split(' ')[0], "%Y-%m-%d").date()

        date_accepted_entry = proj.field('date_application_accepted',
                                         Planning.NAME)
        date_accepted = None
        if date_accepted_entry:
            date_accepted = datetime.strptime(
                date_accepted_entry.split(' ')[0], "%Y-%m-%d").date()

        # Look for the earliest date_opened on an ENT child of a PRJ.
        root = proj.roots[Planning.NAME]
        if root is None or len(root) == 0:
            return (None, None)
        root_entry = root[0].get_latest('record_type')[0]
        if root_entry in _valid_planning_root_type:
            oldest_open = date.max

            num_valid_children = 0
            for child in proj.children[Planning.NAME]:
                record_type = child.get_latest('record_type')[0]
                if record_type not in _valid_planning_ent_codes:
                    continue

                status_value = child.get_latest('status')
                if status_value:
                    status_lower = status_value[0].lower()
                    if any(
                      x in status_lower for x in _invalid_status_keywords):
                        continue

                num_valid_children += 1
                date_opened_field = child.get_latest('date_opened')[0]
                date_opened = datetime.strptime(
                    date_opened_field.split(' ')[0],
                    '%Y-%m-%d').date()
                if date_opened < oldest_open:
                    oldest_open = date_opened

            if num_valid_children == 0:
                return (None, None)

            # If one of the explicit date fields have been marked, use the
            # earliest possible one
            if date_submitted and \
                    date_accepted and \
                    date_accepted < date_submitted:
                return (date_accepted, Planning)
            elif date_submitted:
                return (date_submitted, Planning)
            elif date_accepted:
                return (date_accepted, Planning)

            # If no explicit date field was marked, use the earliest child
            # ENT record
            if oldest_open < date.max:
                return (oldest_open, Planning)

        return (None, None)

    def _entitled_date(self, proj):
        """Use the Entitlements Approved date if it exists. If it doesn't,
        fallback to using the latest closed date of an ENT record.
        """
        date_entitled_entry = proj.field('date_entitlements_approved',
                                         Planning.NAME)
        if date_entitled_entry:
            date_entitled = datetime.strptime(
                date_entitled_entry.split(' ')[0], "%Y-%m-%d").date()
            if date_entitled:
                return (date_entitled, Planning)

        # Look for the ENT child of a PRJ with the latest date_closed
        # (assuming all are closed). Fall back to the PRJ date.
        root = proj.roots[Planning.NAME]
        if root is None or len(root) == 0:
            return (None, None)
        root_entry = root[0].get_latest('record_type')[0]
        if root_entry in _valid_planning_root_type:
            newest_closed = date.min
            count_closed_no_date = 0
            for child in proj.children[Planning.NAME]:
                record_type = child.get_latest('record_type')[0]
                if record_type not in _valid_planning_ent_codes:
                    continue

                status_value = child.get_latest('status')
                if status_value:
                    status_lower = status_value[0].lower()
                    if any(
                      x in status_lower for x in _invalid_status_keywords):
                        continue

                date_closed_value = child.get_latest('date_closed')
                if date_closed_value:
                    date_closed = datetime.strptime(
                        date_closed_value[0].split(' ')[0],
                        "%Y-%m-%d").date()
                    if date_closed > newest_closed:
                        newest_closed = date_closed
                elif status_value and 'closed' in status_value[0].lower():
                    count_closed_no_date += 1
                else:
                    # ENT record is not closed, entitlements not approved
                    return (None, None)

            if newest_closed > date.min:
                return (newest_closed, Planning)
            elif count_closed_no_date > 0:
                # Fall back to PRJ date if all ENT child records are closed
                # but there's no date
                date_closed_entry = root[0].get_latest('date_closed')
                if date_closed_entry:
                    date_closed_field = date_closed_entry[0]
                    if date_closed_field:
                        date_closed = datetime.strptime(
                            date_closed_field.split(' ')[0],
                            '%Y-%m-%d').date()
                        return (date_closed, Planning)
        return (None, None)

    def _filed_for_permits(self, proj):
        filed_for_permits = _get_earliest_date(proj,
                                               'filed_date',
                                               PTS.NAME,
                                               _is_valid_dbi_entry,
                                               "%m/%d/%Y")
        return (filed_for_permits, PTS) \
            if filed_for_permits else (None, None)

    def _under_construction(self, proj):
        """Use the following rules:
        (1) If the permit is a site permit, use
        first_construction_document_date to see it's under construction
        (2) If the permit is not a site permit, use the permit_issued date to
        see if it's under construction
        """
        is_valid_dbi_site_permit = \
            [('permit_type',
             lambda x: x in _valid_dbi_permit_types),
             ('current_status',
             lambda x: x == '' or x not in _invalid_dbi_statuses),
             ('site_permit',
             lambda x: x == 'Y')]
        under_constr = _get_earliest_date(proj,
                                          'first_construction_document_date',
                                          PTS.NAME,
                                          is_valid_dbi_site_permit,
                                          "%m/%d/%Y")
        if not under_constr:
            is_valid_dbi_full_permit = \
                    [('permit_type',
                     lambda x: x in _valid_dbi_permit_types),
                     ('current_status',
                     lambda x: x == '' or x not in _invalid_dbi_statuses),
                     ('site_permit',
                     lambda x: x != 'Y')]
            under_constr = _get_earliest_date(proj,
                                              'issued_date',
                                              PTS.NAME,
                                              is_valid_dbi_full_permit,
                                              "%m/%d/%Y")

        return (under_constr, PTS) \
            if under_constr else (None, None)

    def _completed_construction(self, proj):
        """Use the following rules:
        (1) If the project exists in the TCO record data set, look for a CFC
        or add up all the TCO's to see if all the units have been built
        (2) If not, check if all the associated site permits are complete
        """
        # If a CFC record exists in the TCO dataset then the project has
        # been completed
        date_issued = proj.field('date_issued',
                                 TCO.NAME,
                                 entry_predicate=[('building_permit_type',
                                                   lambda x: x == 'CFC')])
        if date_issued:
            date_entry = datetime.strptime(date_issued, "%Y/%m/%d")
            if date_entry:
                return (date_entry.date(), TCO)

        # If TCO's exist, check if TCO'ed units equal all of the potential
        # units to be built
        tco_units = _get_tco_units(proj)
        dbi_units = _get_dbi_units(proj)

        if dbi_units and dbi_units > 0 and dbi_units == tco_units:
            date = datetime.min
            try:
                fk_entries = proj.fields('date_issued', TCO.NAME)
                for (_, entries) in fk_entries.items():
                    for entry in entries:
                        entry_latest = entry.get_latest('date_issued')
                        date_entry = datetime.strptime(entry_latest[0],
                                                       "%Y/%m/%d")
                        if date_entry > date:
                            date = date_entry
            except ValueError:
                date = datetime.min
                pass

            if date > datetime.min:
                return (date.date(), TCO)

        # If the permits are all complete in PTS we can use the latest date.
        # Check to make sure all permits are actually complete first
        date = datetime.min
        for child in proj.children[PTS.NAME]:
            permit_type = child.get_latest('permit_type')[0]
            if permit_type not in _valid_dbi_permit_types:
                continue

            status_entry = child.get_latest('current_status')
            if not status_entry:
                return (None, None)
            status = status_entry[0]
            if status in _invalid_dbi_statuses:
                continue
            if status != 'complete':
                return (None, None)

            completed_date = child.get_latest('completed_date')
            # If a permit still isn't complete, then the project
            # is ongoing
            if not completed_date[0]:
                return (None, None)

            date_entry = datetime.strptime(completed_date[0],
                                           "%m/%d/%Y")
            if date_entry > date:
                date = date_entry

        return (date.date(), PTS) \
            if date > datetime.min else (None, None)

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

    def _check_and_log_non_sqntl_date(self,
                                      proj,
                                      cur_status,
                                      cur_date,
                                      cur_data,
                                      next_status,
                                      next_date,
                                      next_data):
        if next_date and cur_date and next_date < cur_date:
            self.non_sqntl_dates += 1
            if cur_status not in self.non_sqntl_dates_sample:
                self.non_sqntl_dates_sample[cur_status] = \
                    queue.Queue(maxsize=20)
            if not self.non_sqntl_dates_sample[cur_status].full():
                self.non_sqntl_dates_sample[cur_status].put_nowait(
                    "Project %s has %s date %s fk %s and %s date %s fk %s "
                    "(non-sequential)"
                    % (proj.id,
                       cur_status,
                       cur_date.isoformat(),
                       proj.fk(cur_data.NAME),
                       next_status,
                       next_date.isoformat(),
                       proj.fk(next_data.NAME)))
            return False
        return True

    def _log_non_consecutive_status(self,
                                    proj,
                                    cur_status,
                                    cur_date,
                                    cur_data,
                                    prev_status):
        self.non_consecutive_status += 1
        if cur_status not in self.non_consecutive_status_sample:
            self.non_consecutive_status_sample[cur_status] = \
                queue.Queue(maxsize=20)
        if not self.non_consecutive_status_sample[cur_status].full():
            self.non_consecutive_status_sample[cur_status].put_nowait(
                "Project %s has %s date %s fk %s but no %s date"
                % (proj.id,
                   cur_status,
                   cur_date.isoformat(),
                   proj.fk(cur_data.NAME),
                   prev_status))
        return False

    def rows(self, proj):
        (filed_date, filed_data) = self._under_entitlement_review_date(proj)
        (entitled_date, entitled_data) = self._entitled_date(proj)
        (permits_date, permits_data) = self._filed_for_permits(proj)
        (construction_date, construction_data) = self._under_construction(proj)
        (completed_date, completed_data) = self._completed_construction(proj)

        result = []
        if filed_date:
            self._check_and_log_non_sqntl_date(proj,
                                               "under_entitlement_review",
                                               filed_date,
                                               filed_data,
                                               "entitled",
                                               entitled_date,
                                               entitled_data)
            result.append(
                self.status_row(proj,
                                'under_entitlement_review',
                                filed_date.isoformat(),
                                entitled_date.isoformat() if entitled_date
                                else '',
                                filed_data.OUTPUT_NAME))

        if entitled_date:
            if not filed_date:
                self._log_non_consecutive_status(proj,
                                                 "entitled",
                                                 entitled_date,
                                                 entitled_data,
                                                 "under_entitlement_review")

            # To make these dates sequential we will modify entitled dates
            # so that they don't overlap with dates when filed for permit
            if permits_date and entitled_date > permits_date:
                permits_date = entitled_date

            self._check_and_log_non_sqntl_date(proj,
                                               "entitled",
                                               entitled_date,
                                               entitled_data,
                                               "filed_for_permits",
                                               permits_date,
                                               permits_data)
            result.append(
                self.status_row(proj,
                                'entitled',
                                entitled_date.isoformat(),
                                permits_date.isoformat() if permits_date
                                else '',
                                entitled_data.OUTPUT_NAME))

        if permits_date:
            if not entitled_date:
                if filed_date and \
                        not construction_date and \
                        not completed_date:
                    # Was filed at DBI and Planning around the same time.
                    # To keep statuses mutually exclusive, only return the
                    # Planning statuses.
                    return result
                else:
                    is_adu_checked = proj.field('adu', Planning.NAME)
                    is_legalization_checked = \
                        proj.field('legalization', Planning.NAME)
                    # No need to log ADU projects, these will not show up
                    # in Planning statuses
                    if is_adu_checked.lower() != 'checked' and \
                            is_legalization_checked.lower() != 'checked':
                        self._log_non_consecutive_status(
                            proj,
                            "filed_for_permits",
                            permits_date,
                            permits_data,
                            "entitled")

            self._check_and_log_non_sqntl_date(proj,
                                               "filed_for_permits",
                                               permits_date,
                                               permits_data,
                                               "under_construction",
                                               construction_date,
                                               construction_data)
            result.append(
                self.status_row(proj,
                                'filed_for_permits',
                                permits_date.isoformat(),
                                construction_date.isoformat()
                                if construction_date else '',
                                permits_data.OUTPUT_NAME))
        else:
            return result

        if construction_date:
            if not permits_date:
                self._log_non_consecutive_status(
                    proj,
                    "under_construction",
                    construction_date,
                    construction_data,
                    "filed_for_permits")
            self._check_and_log_non_sqntl_date(
                proj,
                "under_construction",
                construction_date,
                construction_data,
                "completed_construction",
                completed_date,
                completed_data)
            result.append(
                self.status_row(proj,
                                'under_construction',
                                construction_date.isoformat(),
                                completed_date.isoformat()
                                if completed_date else '',
                                construction_data.OUTPUT_NAME))

        if completed_date:
            if not permits_date:
                self._log_non_consecutive_status(
                    proj,
                    "completed_construction",
                    completed_date,
                    completed_data,
                    "filed_for_permits")
            if not construction_date:
                self._log_non_consecutive_status(
                    proj,
                    "completed_construction",
                    completed_date,
                    completed_data,
                    "under_construction")
            result.append(
                self.status_row(proj,
                                'completed_construction',
                                completed_date.isoformat(),
                                '',
                                completed_data.OUTPUT_NAME))
        return result

    def log_bad_data(self):
        if self.non_consecutive_status > 0:
            print('Found %s non-consecutive statuses'
                  % self.non_consecutive_status)
            print('Sample entries:')
            for (status, status_queue) in \
                    self.non_consecutive_status_sample.items():
                print('\tFor status "%s"' % status)
                while not status_queue.empty():
                    sample = status_queue.get_nowait()
                    print('\t\t%s' % sample)

        if self.non_sqntl_dates > 0:
            print('Found %s non-sequential dates' % self.non_sqntl_dates)
            print('Sample entries:')
            for (status, dates_queue) in self.non_sqntl_dates_sample.items():
                print('\tFor status "%s"' % status)
                while not dates_queue.empty():
                    sample = dates_queue.get_nowait()
                    print('\t\t%s' % sample)
