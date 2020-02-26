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
        ])

    def _maybe_add_geo(self, row):
        """Adds 'San Francisco, CA' to an address if necessary.

        PowerBI seems to handle address queries not very well, so even a
        fairly unambiguous '1 South Park 94107' ends up being placed in the
        UK.  The best way to deal with this is explicitly specify
        San Francisco, CA."""
        addr = row[self.index(self.ADDRESS)]
        if not addr:
            return

        if re.search('san francisco', addr, flags=re.IGNORECASE):
            return

        # if we match a 'CA', we only skip adding geo information if it
        # comes at the end or is followed by a zip-code like entry.
        # Explicitly do not ignore case here, because 'ca' is a bit ambiguous
        if re.search('CA$|CA [0-9]{5}(-[0-9]{4})?', addr):
            return

        if not re.search('[0-9]{5}', addr):
            row[self.index(self.ADDRESS)] = addr.strip() + \
                ', San Francisco, CA'
        else:
            row[self.index(self.ADDRESS)] = \
                re.sub(r',? +([0-9]{5}(-[0-9]{4})?)',
                       r', San Francisco, CA \1',
                       addr)

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

            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.fk(mohcd)
            row[self.index(self.PERMIT_AUTHORITY)] = 'mohcd'  # TODO

        if used_mohcd:
            return

        addr = proj.field('address', Planning.NAME)
        name = proj.field('name', Planning.NAME)
        if name or addr:
            if not name:
                name = re.sub(' [0-9]{5}$', '', addr)

            row[self.index(self.NAME)] = name
            row[self.index(self.ADDRESS)] = addr
            row[self.index(self.APPLICANT)] = ''  # TODO
            row[self.index(self.SUPERVISOR_DISTRICT)] = ''  # TODO
            row[self.index(self.PERMIT_AUTHORITY)] = Planning.OUTPUT_NAME
            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.fk(Planning.NAME)
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
            row[self.index(self.PERMIT_AUTHORITY)] = PTS.NAME
            row[self.index(self.PERMIT_AUTHORITY_ID)] = proj.fk(
                    PTS.NAME, entry_predicate=_is_valid_dbi_entry)

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
        if mohcd is not None:
            net, bmr, source = mohcd
            row[self.index(self.NET_NUM_UNITS)] = str(net)
            row[self.index(self.NET_NUM_UNITS_DATA)] = source
            row[self.index(self.NET_NUM_UNITS_BMR)] = str(bmr)
            row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = source
        else:
            dbi_net = _get_dbi_units(proj)
            planning_net = proj.field(
                'number_of_market_rate_units', Planning.NAME)
            net = dbi_net
            # PTS may have an explicitly set 0 unit count for projects
            # that have no business dealing with housing (possible with
            # permit type 3 in particular), so we only emit a 0-count
            # PTS unit count if we had an explicit non-0 Planning unit
            # count (therefore indicating a housing-related project that
            # lost its housing somehow).
            if (dbi_net is not None
                    and (dbi_net != 0 or planning_net)):
                row[self.index(self.NET_NUM_UNITS)] = str(dbi_net)
                row[self.index(self.NET_NUM_UNITS_DATA)] = PTS.OUTPUT_NAME
            else:
                try:
                    net = int(planning_net)
                    row[self.index(self.NET_NUM_UNITS)] = planning_net
                    row[self.index(self.NET_NUM_UNITS_DATA)] = \
                        Planning.OUTPUT_NAME
                except ValueError:
                    net = None
                    pass
            bmr_net = proj.field('number_of_affordable_units', Planning.NAME)
            if bmr_net != '':
                row[self.index(self.NET_NUM_UNITS_BMR)] = bmr_net
                row[self.index(self.NET_NUM_UNITS_BMR_DATA)] = \
                    Planning.OUTPUT_NAME
            elif net is not None:
                row[self.index(self.NET_EST_NUM_UNITS_BMR)] = \
                    self._estimate_bmr(net)
                row[self.index(self.NET_EST_NUM_UNITS_BMR_DATA)] = \
                    Planning.OUTPUT_NAME

    def _atleast_one_measure(self, row):
        return (row[self.index(self.NET_NUM_UNITS)] != '' or
                row[self.index(self.NET_NUM_UNITS_BMR)] != '' or
                row[self.index(self.NET_EST_NUM_UNITS_BMR)] != '')

    _UNTRUSTED_EMPTY_ADDRESS_ZERO_UNITS = set([Planning.NAME, PTS.NAME])

    def _nonzero_or_nonempty_address(self, row):
        """Returns true if this row had a non-empty address, or had an
        empty address but a non-zero net unit count from planning or dbi"""
        if row[self.index(self.ADDRESS)] != '':
            return True

        if (row[self.index(self.NET_NUM_UNITS_DATA)] not in
            self._UNTRUSTED_EMPTY_ADDRESS_ZERO_UNITS or
            (row[self.index(self.NET_NUM_UNITS)] and
             row[self.index(self.NET_NUM_UNITS)] != '0')):
            return True
        return False

    def rows(self, proj):
        row = [''] * len(self.header())

        self.gen_id(row, proj)
        self._gen_facts(row, proj)
        self._gen_units(row, proj)
        self._maybe_add_geo(row)

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

    def rows(self, proj):
        result = []
        self._geom(result, proj)
        return result


class ProjectUnitCountsFull(NameValueTable):
    def __init__(self):
        super().__init__('project_unit_counts_full')

    def _all_units(self, rows, proj):
        planning_units = proj.field(
            'number_of_market_rate_units', Planning.NAME)
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
        date = proj.field('earliest_addenda_arrival',
                          PermitAddendaSummary.NAME)
        if date:
            rows.append(self.nv_row(proj,
                                    name='earliest_addenda_arrival',
                                    value=date,
                                    data=PermitAddendaSummary.OUTPUT_NAME))

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

        self._unique(result)

        return result


class ProjectStatusHistory(Table):
    _Planning_ENT_CODES = {'ENV', 'AHB', 'COA', 'CUA', 'CTZ', 'DNX', 'ENX',
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
        # Planning data pipeline (if that doesn't exist fall back to using our
        # own logic)
        ppa_opened_field = proj.field(
            'date_opened', Planning.NAME,
            entry_predicate=[('record_type', lambda x: x == 'PPA')])
        if ppa_opened_field:
            ppa_opened_date = datetime.strptime(
                ppa_opened_field.split(' ')[0],
                "%d-%b-%y").date()
            return (ppa_opened_date.isoformat(), Planning.OUTPUT_NAME)

        return ('', None)

    def _filed_for_entitlements_date(self, proj):
        # TODO: Use the Application Submitted date once we have pulled
        # in the new Planning data pipeline (if that doesn't exist fall back to
        # our own logic)

        # Look for the earliest date_opened on an ENT child of a PRJ.
        root = proj.roots[Planning.NAME]
        if root is None:
            print("Error: Project with non-Planning root id %s" % proj.id)
            return ('', None)
        if root[0].get_latest('record_type')[0] == 'PRJ':
            oldest_open = date.max
            for child in proj.children[Planning.NAME]:
                record_type = child.get_latest('record_type')[0]
                if record_type not in self._Planning_ENT_CODES:
                    continue

                date_opened_field = child.get_latest('date_opened')[0]
                date_opened = datetime.strptime(
                    date_opened_field.split(' ')[0],
                    '%d-%b-%y').date()
                if date_opened < oldest_open:
                    oldest_open = date_opened

            if oldest_open < date.max:
                return (oldest_open.isoformat(), Planning.OUTPUT_NAME)

        return ('', None)

    def _entitled_date(self, proj):
        # TODO: Use the Entitlements Approved date once we have pulled
        # in the new Planning data pipeline (if that doesn't exist fall back)

        # Look for the ENT child of a PRJ with the latest date_closed
        # (assuming all are closed). Fall back to the PRJ date.
        root = proj.roots[Planning.NAME]
        if root is None:
            print("Error: Project with non-Planning root id %s" % proj.id)
            return ('', None)
        if root[0].get_latest('record_type')[0] == 'PRJ':
            newest_closed = date.min
            count_closed_no_date = 0
            for child in proj.children[Planning.NAME]:
                record_type = child.get_latest('record_type')[0]
                if record_type not in self._Planning_ENT_CODES:
                    continue

                date_closed_value = child.get_latest('date_closed')
                status_value = child.get_latest('status')
                if date_closed_value:
                    date_closed = datetime.strptime(
                        date_closed_value[0].split(' ')[0],
                        "%d-%b-%y").date()
                    if date_closed > newest_closed:
                        newest_closed = date_closed
                elif status_value and 'closed' in status_value[0].lower():
                    count_closed_no_date += 1
                else:
                    # ENT record is not closed, entitlements not approved
                    return ('', None)

            if newest_closed > date.min:
                return (newest_closed.isoformat(), Planning.OUTPUT_NAME)
            elif count_closed_no_date > 0:
                # Fall back to PRJ date if all ENT child records are closed
                # but there's no date
                date_closed_field = root[0].get_latest('date_closed')
                if date_closed_field:
                    date_closed = datetime.strptime(
                        date_closed_field[0].split(' ')[0],
                        '%d-%b-%y').date()
                    return (date_closed.isoformat(), Planning.OUTPUT_NAME)

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
