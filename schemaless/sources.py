# Lint as: python3
"""Source class to define the interface for reading a source file."""
from csv import DictReader
from datetime import date
from datetime import datetime
from fileutils import open_file
import logging

import schemaless.mapblklot_generator as mapblklot_gen
from scourgify.exceptions import AddressNormalizationError
from scourgify.normalize import format_address_record
from scourgify.normalize import normalize_address_record


logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


class Field:
    def get_value(self, record):
        pass

    def get_value_str(self, record):
        return str(self.get_value(record))


class PrimaryKey(Field):
    def __init__(self, prefix, *fields):
        self.prefix = prefix
        self.fields = fields

    def __str__(self):
        return "_".join([self.prefix] + self.fields)

    def get_value(self, record):
        vals = []
        for field in self.fields:
            if isinstance(field, Field):
                vals.append(field.get_value_str(record))
            else:
                val = record.get(field)
                if val:
                    vals.append(val)
                else:
                    logger.warning(
                        "Field used to construct PK for this record "
                        "is null: %s", field)
                    vals.append("")
        return "_".join([self.prefix] + vals)


class Concat(Field):
    def __init__(self, *fields):
        self.fields = fields

    def __str__(self):
        return "Concat(%s)" % ", ".join(self.fields)

    def get_value(self, record):
        vals = []
        for field in self.fields:
            if isinstance(field, Field):
                val = field.get_value_str(record)
            else:
                val = record.get(field)
            if val:
                vals.append(val)
        return "_".join(vals)


class Date(Field):
    def __init__(self, field, date_format):
        self.field = field
        self.date_format = date_format

    def __str__(self):
        return "%s (%s)" % (self.field, self.date_format)

    def get_value(self, record):
        try:
            return datetime.strptime(
                record[self.field].split(" ")[0], self.date_format).date()
        except ValueError:
            return None

    def get_value_str(self, record):
        return self.get_value(record).isoformat()


class Mapblklot(Field):
    def __init__(self, block=None, lot=None, blklot=None, mapblklot=None):
        self.block = block
        self.lot = lot
        self.blklot = blklot
        self.mapblklot = mapblklot

    def get_value(self, record):
        if self.mapblklot:
            return record[self.mapblklot]

        mapblklot_generator = \
            mapblklot_gen.MapblklotGeneratorSingleton.get_instance()
        if mapblklot_generator:
            if self.blklot:
                return mapblklot_generator.find_mapblklot_for_blklot(
                    record[self.blklot])
            if self.block and self.lot:
                return mapblklot_generator.find_mapblklot_for_blklot(
                    record[self.block] + record[self.lot])
        else:
            raise MapblklotException(
                "MapblklotGeneratorSingleton is not instantiated. Please "
                "instantiate by calling "
                "schemaless.mapblklot_generator.init(filepath) in top-level "
                "script environment")
        return None


class MapblklotException(Exception):
    pass


class Address(Field):
    def __init__(self, *fields):
        self.fields = fields

    def __str__(self):
        return " ".join(self.fields)

    def get_value(self, record):
        vals = []
        for field in self.fields:
            if isinstance(field, Field):
                vals.append(field.get_value_str(record))
            else:
                vals.append(record.get(field, ""))
        addr_str = " ".join(vals).strip().title()
        if not addr_str:
            return ""

        try:
            addr = normalize_address_record(addr_str)
        except AddressNormalizationError:
            logger.warning("Unparseable: %s", addr_str)
            return ""
        else:
            if not addr['postal_code']:
                # We need this to normalize again, and we can't guess it
                try:
                    return format_address_record(addr)
                except AddressNormalizationError:
                    logger.warning("Unable to format address %s", addr)
                    return ""

            if not addr['city']:
                addr['city'] = "San Francisco"
            if not addr['state']:
                addr['state'] = "California"

        # There are a lot of addresses that look like "123 Main St 94102". This
        # gets parsed into a dict that looks like:
        #
        #     {'address_line_1': '123 MAIN ST',
        #      'address_line_2': None,
        #      'city': None,
        #      'state': None,
        #      'postal_code': '94102'}
        #
        # We want city and state to be populated, too, so we add them in and
        # then normalize once more to ensure it's still valid.
        try:
            addr = normalize_address_record(addr)
        except AddressNormalizationError:
            logger.warning("Unparseable: %s", addr)
            return ""
        else:
            try:
                return format_address_record(addr)
            except AddressNormalizationError:
                logger.warning("WARN4: Unable to parse address %s for %s",
                               addr, addr_str)
                return ""
        return ""


class Source:
    NAME = 'Base Class'
    FK = PrimaryKey(NAME, 'None')
    DATE = Date('None', '%m/%d/%Y')

    def __init__(self, filepath):
        self._filepath = filepath

    @classmethod
    def foreign_key(cls, record):
        return cls.FK.get_value(record)

    @classmethod
    def calculated_fields(self, record):
        ret = {}
        for key, field in self.COMPUTED_FIELDS.items():
            val = field.get_value_str(record)
            if val:
                ret[key] = val.strip()
        return ret

    @classmethod
    def field_names(cls):
        pass

    def yield_records(self):
        pass


class DirectSource(Source):
    """Superclass for sources where fields map directly to fields in the input
    file."""

    # Mapping of field names in the input file to the field names to be used in
    # the records for this source.
    FIELDS = {}
    COMPUTED_FIELDS = {}

    @classmethod
    def field_names(cls):
        return set(cls.FIELDS.values())

    def yield_records(self):
        with open_file(self._filepath,
                       mode='rt',
                       encoding='utf-8',
                       errors='replace') as inf:
            reader = DictReader(inf)

            for line in reader:
                ret = {self.FIELDS[key]: val.strip()
                       for key, val in line.items()
                       if key in self.FIELDS and val}
                ret.update(self.calculated_fields(ret))
                yield ret


class Planning(DirectSource):
    NAME = 'planning'
    FK = PrimaryKey(NAME, 'record_id')
    DATE = Date('date_opened', '%m/%d/%Y')
    OUTPUT_NAME = 'planning'
    FIELDS = {
        'RECORD_ID': 'record_id',
        'RECORD_TYPE': 'record_type',
        'RECORD_STATUS': 'status',
        'CLASS': 'class',
        'PROJECT_NAME': 'name',
        'PROJECT_ADDRESS': 'address',
        'DESCRIPTION': 'description',

        # Location
        'BLOCK': 'block',
        'LOT': 'lot',
        'MAPBLOCKLOT': 'mapblocklot',
        'WKT_MULTIPOLYGON': 'wkt_multipolygon',
        'point': 'point',
        'SUPERVISOR_DISTRICT': 'supervisor_district',

        # Related records
        'PARENT_ID': 'parent',
        # TODO: children?
        'RELATED_BUILDING_PERMIT': 'building_permits',

        # Dates
        'OPEN_DATE': 'date_opened',
        'CLOSE_DATE': 'date_closed',
        'DATE_APPLICATION_SUBMITTED': 'date_application_submitted',
        'DATE_ENTITLEMENTS_APPROVED': 'date_entitlements_approved',
        'DATE_PPA_SUBMITTED': 'date_ppa_submitted',
        'DATE_APPLICATION_ACCEPTED': 'date_application_accepted',
        'DATE_PPA_LETTER_ISSUED': 'date_ppa_letter_issued',
        'DATE_NIA_ISSUED': 'date_nia_issued',
        'DATE_PLAN_CHECK_LETTER_ISSUED': 'date_plan_check_letter_issued',
        'DATE_PROJECT_DESC_STABLE': 'date_project_desc_stable',
        'DATE_OF_FIRST_HEARING': 'date_of_first_hearing',
        'DATE_OF_FINAL_HEARING': 'date_of_final_hearing',

        # Developer and planner
        'DEVELOPER_NAME': 'developer_name',
        'DEVELOPER_ORG': 'developer_org',
        'ASSIGNED_TO_PLANNER': 'assigned_to_planner',
        'PLANNER_FIRST_NAME': 'planner_first_name',
        'PLANNER_LAST_NAME': 'planner_last_name',
        'PLANNER_EMAIL': 'planner_email',
        'PLANNER_PHONE': 'planner_phone',

        'ENVIRONMENTAL_REVIEW_TYPE': 'environmental_review_type',
        'CHANGE_OF_USE': 'change_of_use',
        'ADDITIONS': 'additions',
        'NEW_CONSTRUCTION': 'new_construction',
        'LEG_ZONE_CHANGE': 'leg_zone_change',

        # Incentives/Bonuses
        'SB35': 'sb35',
        'SB330': 'sb330',
        'AB2162': 'ab2162',
        'HOUSING_SUSTAINABILITY_DIST': 'housing_sustainability_dist',
        'HOMESF': 'homesf',
        'STATE_DENSITY_BONUS_ANALYZED': 'state_density_bonus_analyzed',
        'STATE_DENSITY_BONUS_INDIVIDUAL': 'state_density_bonus_individual',
        'BASE_DENSITY': 'base_density',
        'BONUS_DENSITY': 'bonus_density',

        # Unit info
        'SENIOR': 'senior',
        'AFFORDABLE_UNITS': 'affordable_units',
        'STUDENT': 'student',
        'INCLUSIONARY': 'inclusionary',
        'ADU': 'adu',
        'LEGALIZATION': 'legalization',
        'CHANGE_OF_DWELLING_UNITS': 'change_of_dwelling_units',

        'NUMBER_OF_UNITS': 'number_of_units',
        'NUMBER_OF_MARKET_RATE_UNITS': 'number_of_market_rate_units',
        'NUMBER_OF_AFFORDABLE_UNITS': 'number_of_affordable_units',

        'RENTAL_UNITS': 'rental_units',
        'OWNERSHIP_UNITS': 'ownership_units',
        'UNKOWN_UNITS': 'unkown_units',

        'INCLUSIONARY_PERCENT': 'inclusionary_percent',
        'AHBP_100_PERCENT_AFFORDABLE': 'ahbp_100_percent_affordable',

        'RESIDENTIAL_EXIST': 'residential_exist',
        'RESIDENTIAL_PROP': 'residential_prop',
        'RESIDENTIAL_STUDIO_EXIST': 'residential_units_studio_exist',
        'RESIDENTIAL_STUDIO_PROP': 'residential_units_studio_prop',
        'RESIDENTIAL_1BR_EXIST': 'residential_units_1br_exist',
        'RESIDENTIAL_1BR_PROP': 'residential_units_1br_prop',
        'RESIDENTIAL_2BR_EXIST': 'residential_units_2br_exist',
        'RESIDENTIAL_2BR_PROP': 'residential_units_2br_prop',
        'RESIDENTIAL_3BR_EXIST': 'residential_units_3br_exist',
        'RESIDENTIAL_3BR_PROP': 'residential_units_3br_prop',
        # ADU is an accessory dwelling unit (aka casita aka granny flat)
        'RESIDENTIAL_ADU_STUDIO_EXIST': 'residential_units_adu_studio_exist',
        'RESIDENTIAL_ADU_STUDIO_PROP': 'residential_units_adu_studio_prop',
        'RESIDENTIAL_ADU_1BR_EXIST': 'residential_units_adu_1br_exist',
        'RESIDENTIAL_ADU_1BR_PROP': 'residential_units_adu_1br_prop',
        'RESIDENTIAL_ADU_2BR_EXIST': 'residential_units_adu_2br_exist',
        'RESIDENTIAL_ADU_2BR_PROP': 'residential_units_adu_2br_prop',
        'RESIDENTIAL_ADU_3BR_EXIST': 'residential_units_adu_3br_exist',
        'RESIDENTIAL_ADU_3BR_PROP': 'residential_units_adu_3br_prop',
        # GH is a group home
        'RESIDENTIAL_GH_ROOMS_EXIST': 'residential_units_gh_rooms_exist',
        'RESIDENTIAL_GH_ROOMS_PROP': 'residential_units_gh_rooms_prop',
        'RESIDENTIAL_GH_BEDS_EXIST': 'residential_units_gh_beds_exist',
        'RESIDENTIAL_GH_BEDS_PROP': 'residential_units_gh_beds_prop',
        # SRO is a single-room occupancy
        'RESIDENTIAL_SRO_EXIST': 'residential_units_sro_exist',
        'RESIDENTIAL_SRO_PROP': 'residential_units_sro_prop',
        # Micro is a micro-unit (typically 200sq.ft. and fewer)
        'RESIDENTIAL_MICRO_EXIST': 'residential_units_micro_exist',
        'RESIDENTIAL_MICRO_PROP': 'residential_units_micro_prop',

        # Other info about the property, not relevant to the dashboard but
        # possibly interesting for third parties.
        'PARKING_SPACES_EXIST': 'parking_spaces_exist',
        'PARKING_SPACES_PROP': 'parking_spaces_prop',
        'CAR_SHARE_SPACES_EXIST': 'car_share_spaces_exist',
        'CAR_SHARE_SPACES_PROP': 'car_share_spaces_prop',
        'PARKING_GSF_EXIST': 'parking_gsf_exist',
        'PARKING_GSF_PROP': 'parking_gsf_prop',
        'RETAIL_COMMERCIAL_EXIST': 'retail_commercial_exist',
        'RETAIL_COMMERCIAL_PROP': 'retail_commercial_prop',
        'OFFICE_EXIST': 'office_exist',
        'OFFICE_PROP': 'office_prop',
        'INDUSTRIAL_PDR_EXIST': 'industrial_pdr_exist',
        'INDUSTRIAL_PDR_PROP': 'industrial_pdr_prop',
        'MEDICAL_EXIST': 'medical_exist',
        'MEDICAL_PROP': 'medical_prop',
        'VISITOR_EXIST': 'visitor_exist',
        'VISITOR_PROP': 'visitor_prop',
        'CIE_EXIST': 'cie_exist',
        'CIE_PROP': 'cie_prop',
    }
    COMPUTED_FIELDS = {
        'address_norm': Address('address'),
        'blocklot': Concat('block', 'lot'),
    }
    # This dataset is not public yet.
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/SF-Planning-Permitting-Data/kncr-c6jw"  # NOQA
    DATA_SF_VIEW_ID = "kncr-c6jw"


class PTS(DirectSource):
    NAME = 'pts'
    FK = PrimaryKey(NAME, 'record_id')
    DATE = Date('filed_date', '%m/%d/%Y')
    OUTPUT_NAME = 'dbi'
    FIELDS = {
        'Record ID': 'record_id',
        'Permit Number': 'permit_number',
        'Permit Type': 'permit_type',
        'Permit Type Definition': 'permit_type_definition',
        'Permit Creation Date': 'permit_creation_date',
        'Block': 'block',
        'Lot': 'lot',
        'Street Number': 'street_number',
        'Street Number Suffix': 'street_number_suffix',
        'Street Name': 'street_name',
        'Street Name Suffix': 'street_name_suffix',
        'Unit': 'unit',
        'Unit Suffix': 'unit_suffix',
        'Zipcode': 'zipcode',
        'Location': 'location',
        'Supervisor District': 'supervisor_district',
        'Current Status': 'current_status',
        'Current Status Date': 'current_status_date',
        'Filed Date': 'filed_date',
        'Issued Date': 'issued_date',
        'Completed Date': 'completed_date',
        'First Construction Document Date': 'first_construction_document_date',
        'Permit Expiration Date': 'permit_expiration_date',
        'Existing Use': 'existing_use',
        'Proposed Use': 'proposed_use',
        'Existing Units': 'existing_units',
        'Proposed Units': 'proposed_units',
        'Existing Construction Type': 'existing_construction_type',
        'Existing Construction Type Description':
        'existing_construction_type_description',
        'Proposed Construction Type': 'proposed_construction_type',
        'Proposed Construction Type Description':
        'proposed_construction_type_description',
        'Site Permit': 'site_permit',
    }
    COMPUTED_FIELDS = {
        'address_norm': Address(
            'street_number',
            'street_number_suffix',
            'street_name',
            'street_name_suffix',
            'unit',
            'unit_suffix',
            'zipcode',
        ),
        'mapblklot': Mapblklot('block', 'lot'),
    }
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9"  # NOQA
    DATA_SF_VIEW_ID = "i98e-djp9"


class TCO(DirectSource):
    NAME = 'tco'
    OUTPUT_NAME = NAME
    DATE = Date('date_issued', '%Y/%m/%d')
    FK = PrimaryKey(NAME, 'building_permit_number', DATE)
    FIELDS = {
        'Building Permit Application Number': 'building_permit_number',
        'Building Address': 'address',
        'Date Issued': 'date_issued',
        'Document Type': 'building_permit_type',
        'Number of Units Certified': 'num_units',
    }
    COMPUTED_FIELDS = {
        'address_norm': Address('address'),
    }
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Dwelling-Unit-Completion-Counts-by-Building-Permit/j67f-aayr"  # NOQA
    DATA_SF_VIEW_ID = "j67f-aayr"


class MOHCDInclusionary(DirectSource):
    NAME = 'mohcd_inclusionary'
    OUTPUT_NAME = NAME
    FK = PrimaryKey(NAME, 'project_id')
    FIELDS = {
        'Project ID': 'project_id',
        'Project Status': 'project_status',
        'Project Name': 'project_name',
        'Street Number': 'street_number',
        'Street Name': 'street_name',
        'Street Type': 'street_type',
        'Zip Code': 'zip_code',
        'Housing Tenure': 'housing_tenure',
        'Section 415 Declaration': 'section_415_declaration',
        'Entitlement Approval Date': 'entitlement_approval_date',
        'Actual/Estimated Completion Date':
        'date_estimated_or_actual_completion',
        'Planning Case Number': 'planning_case_number',
        'Planning Entitlements': 'planning_entitlements',
        'Project Units': 'total_project_units',
        'Affordable Units': 'total_affordable_units',
        'Units Subject to Section 415': 'units_subject_to_415_declaration',
        'On-Site Affordable Units': 'on_site_affordable_units',
        'Off-Site Affordable Units': 'off_site_affordable_units',
        'Off-Site Affordable Units at This Site':
        'off_site_affordable_units_at_site',
        'SRO Units': 'num_sro_units',
        'Studio Units': 'num_studio_units',
        '1bd Units': 'num_1bd_units',
        '2bd Units': 'num_2bd_units',
        '3bd Units': 'num_3bd_units',
        '4bd Units': 'num_4bd_units',
        '30% AMI': 'num_30_percent_ami_units',
        '50% AMI': 'num_50_percent_ami_units',
        '55% AMI': 'num_55_percent_ami_units',
        '60% AMI': 'num_60_percent_ami_units',
        '80% AMI': 'num_80_percent_ami_units',
        '90% AMI': 'num_90_percent_ami_units',
        '100% AMI': 'num_100_percent_ami_units',
        '120% AMI': 'num_120_percent_ami_units',
        '150% AMI': 'num_150_percent_ami_units',
        'Supervisor District': 'supervisor_district',
        'Location': 'location',
    }
    COMPUTED_FIELDS = {
        'address_norm': Address(
            'street_number',
            'street_name',
            'street_type',
            'zip_code',
        ),
    }
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Residential-Projects-With-Inclusionary-Requirement/nj3x-rw36"  # NOQA
    DATA_SF_VIEW_ID = "nj3x-rw36"


class MOHCDPipeline(DirectSource):
    NAME = 'mohcd_pipeline'
    OUTPUT_NAME = NAME
    FK = PrimaryKey(NAME, 'project_id')
    FIELDS = {
        'Project ID': 'project_id',
        'Project Status': 'project_status',
        'Project Name': 'project_name',
        'Street Number': 'street_number',
        'Street Name': 'street_name',
        'Street Type': 'street_type',
        'Zip Code': 'zip_code',
        'Supervisor District': 'supervisor_district',
        'Location': 'location',  # This is a POINT()
        'Project Lead Sponsor': 'project_lead_sponsor',
        # 'Project Co-Sponsor': 'project_co_sponsor',  # TODO: Not included?
        'Project Owner': 'project_owner',
        'Lead Agency': 'lead_agency',
        'Program Area': 'program_area',
        # 'Project Area': 'project_area',  # TODO: Not included?
        'Project Type': 'project_type',
        'Housing Tenure': 'housing_tenure',
        'Issuance of Notice to Proceed': 'date_issuance_of_notice_to_proceed',
        'Issuance of Building Permit': 'date_issuance_of_building_permit',
        'Issuance of First Construction Document': (
                'date_issuance_of_first_construction_document'),
        'Estimated/Actual Construction Start Date': (
                'date_estimated_or_actual_actual_construction_start'),
        'Estimated Construction Completion': (
                'date_estimated_construction_completion'),
        # NOTE: There are actually two spaces in 'Planning  Case Number' in
        # the source dataset.
        # planning_case_number is the PPTS record_id
        'Planning  Case Number': 'planning_case_number',
        # 'Property Informaiton Map Link': 'property_informaiton_map_link',
        'Planning Entitlements': 'planning_entitlements',
        # 'Entitlement Approval': 'entitlement_approval',
        'Section 415 Declaration': 'section_415_declaration',
        'Project Units': 'total_project_units',
        'Affordable Units': 'total_affordable_units',
        'Market Rate Units': 'total_market_rate_units',
        '% Affordable': 'percent_affordable',
        'SRO Units': 'num_sro_units',
        'Studio Units': 'num_studio_units',
        '1bd Units': 'num_1bd_units',
        '2bd Units': 'num_2bd_units',
        '3bd Units': 'num_3bd_units',
        '4bd Units': 'num_4bd_units',
        '5+ bd Units': 'num_5_plus_bd_units',
        # 'Mobility Units': 'mobility_units',
        # 'Manager Units': 'manager_units',
        # 'Manager Unit(s) Type': 'manager_unit(s) type',

        # TODO: I think we might care about these unit types
        # 'Family Units': 'family_units',
        # 'Senior Units': 'senior_units',
        # 'TAY Units': 'tay_units',
        # 'Homeless Units': 'homeless_units',
        # 'Disabled Units': 'disabled_units',
        # 'LOSP Units': 'losp_units',
        # 'Public Housing Replacement Units': \
        #     'public_housing_replacement_units',
        '20% AMI': 'num_20_percent_ami_units',
        '30% AMI': 'num_30_percent_ami_units',
        '40% AMi': 'num_40_percent_ami_units',
        '50% AMI': 'num_50_percent_ami_units',
        '55% AMI': 'num_55_percent_ami_units',
        '60% AMI': 'num_60_percent_ami_units',
        '80% AMI': 'num_80_percent_ami_units',
        '90% AMI': 'num_90_percent_ami_units',
        '100% AMI': 'num_100_percent_ami_units',
        '105% AMI': 'num_105_percent_ami_units',
        '110% AMI': 'num_110_percent_ami_units',
        '120% AMI': 'num_120_percent_ami_units',
        '130% AMI': 'num_130_percent_ami_units',
        '150% AMI': 'num_150_percent_ami_units',
        'AMI Undeclared': 'num_ami_undeclared_units',
        # 'Latitude': 'latitude',
        # 'Longitude': 'longitude',
    }
    COMPUTED_FIELDS = {
        'address_norm': Address(
            'street_number',
            'street_name',
            'street_type',
            'zip_code',
        ),
    }
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Affordable-Housing-Pipeline/aaxw-2cb8"  # NOQA
    DATA_SF_VIEW_ID = "aaxw-2cb8"


class AffordableRentalPortfolio(DirectSource):
    """MOHCD/OCII's affordable rental portfolio"""
    NAME = 'bmr'
    DATE = Date('year_affordability_began', '%Y')
    FK = PrimaryKey(NAME, 'project_id')
    OUTPUT_NAME = NAME
    FIELDS = {
        'Project ID': 'project_id',
        'Project Name': 'project_name',
        'Street Number': 'street_number',
        'Street Name': 'street_name',
        'Street Type': 'street_type',
        'Zip Code': 'zip_code',
        'Location': 'location',
        'Supervisor District': 'supervisor_district',
        'Project Sponsor': 'project_sponsor',
        'Total Units': 'total_units',
        'Total Beds': 'total_beds',
        'Affordable Units': 'total_affordable_units',
        'Affordable Beds': 'total_affordable_beds',
        'Single Room Occupancy Units': 'num_sro_units',
        'Studio Units': 'num_studio_units',
        '1bd Units': 'num_1bd_units',
        '2bd Units': 'num_2bd_units',
        '3bd Units': 'num_3bd_units',
        '4bd Units': 'num_4bd_units',
        '5+ bd Units': 'num_5_plus_bd_units',
        'Family Units': 'num_family_units',
        'Senior Units': 'num_senior_units',
        'TAY Units': 'num_tay_units',
        'Homeless Units': 'num_homeless_units',
        'LOSP Units': 'num_losp_units',
        'Disabled Units': 'num_disabled_units',
        '20% AMI': 'num_20_percent_ami_units',
        '30% AMI': 'num_30_percent_ami_units',
        '40% AMI': 'num_40_percent_ami_units',
        '50% AMI': 'num_50_percent_ami_units',
        '60% AMI': 'num_60_percent_ami_units',
        '80% AMI': 'num_80_percent_ami_units',
        '120% AMI': 'num_120_percent_ami_units',
        'More than 120% AMI': 'num_more_than_120_percent_ami_units',
        'Year Building Constructed': 'year_constructed',
        'Year Affordability Began': 'year_affordability_began',
    }
    COMPUTED_FIELDS = {
        'address_norm': Address(
            'street_number',
            'street_name',
            'street_type',
            'zip_code',
        ),
    }
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Mayor-s-Office-of-Housing-and-Community-Developmen/9rdx-httc"  # NOQA
    DATA_SF_VIEW_ID = "9rdx-httc"


class OEWDPermits(DirectSource):
    NAME = 'oewd_permits'
    OUTPUT_NAME = NAME
    FK = PrimaryKey(
        NAME, 'row_number', 'delivery_agency', 'project_name',
        'phase_bldg_address_blklot')
    FIELDS = {
        'Row No.': 'row_number',
        'Housing Delivery Agency': 'delivery_agency',
        'Project Name:': 'project_name',
        'Phase/ Bldg/Address/B+L': 'phase_bldg_address_blklot',
        'Building Permit No.': 'permit_number',
        'Total Units': 'total_units',
        '100% Affordable Units': 'affordable_units',
        'Permit Type': 'permit_type',
        'DBI Arrival/ Intake Date': 'dbi_arrival_date',
        'Target Permit Issuance Date': 'target_permit_issuance_date',
    }
    DATA_SF = "https://data.sfgov.org/dataset/Priority-Permits/336t-bzzm"
    DATA_SF_VIEW_ID = "336t-bzzm"


class PermitAddendaSummary(Source):
    '''Class for permit addenda summary data.

    Note that the fields of this source do *not* necessarily map directly to
    fields from the input file (unlike subclasses of DirectSource).

    Instead, we look at all the addenda pertaining to a given permit, summarize
    the  data into a few key fields, and output one record per permit with
    those key fields.
    '''
    NAME = 'permit_addenda_summary'
    OUTPUT_NAME = NAME
    FK = PrimaryKey(NAME, 'permit_number')

    FIELDS = [
        'permit_number',
        'earliest_addenda_arrival'
    ]
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Department-of-Building-Inspection-Permit-Addenda-w/87xy-gk8d"  # NOQA
    DATA_SF_VIEW_ID = "87xy-gk8d"

    @classmethod
    def field_names(cls):
        return cls.FIELDS

    def yield_records(self):
        '''Outputs one record per permit number with a few key fields summarizing
        info about related addenda.'''
        with open_file(self._filepath,
                       mode='rt',
                       encoding='utf-8',
                       errors='replace') as inf:
            reader = DictReader(inf)

            permit_to_arrival = {}
            for line in reader:
                permit_number = line['APPLICATION_NUMBER']
                try:
                    arrive_date = datetime.strptime(
                        line['ARRIVE'].split(" ")[0], '%Y/%m/%d').date()
                except ValueError:
                    arrive_date = date.max
                if permit_number not in permit_to_arrival or \
                        arrive_date < permit_to_arrival[permit_number]:
                    permit_to_arrival[permit_number] = arrive_date

            for permit_number in permit_to_arrival.keys():
                arrive_date = permit_to_arrival[permit_number]
                arrive_date_str = arrive_date.isoformat() \
                    if arrive_date != date.max else ''
                yield {
                    'permit_number': permit_number,
                    'earliest_addenda_arrival': arrive_date_str
                }


source_map = {
    Planning.NAME: Planning,
    PTS.NAME: PTS,
    TCO.NAME: TCO,
    MOHCDPipeline.NAME: MOHCDPipeline,
    MOHCDInclusionary.NAME: MOHCDInclusionary,
    PermitAddendaSummary.NAME: PermitAddendaSummary,
    AffordableRentalPortfolio.NAME: AffordableRentalPortfolio,
    OEWDPermits.NAME: OEWDPermits
}

# This is separate because, while we depend on the parcels data to tie blocklots
# to mapblocklots, it's not actually a proper Source because its data doesn't
# make it into the schemaless output file.
PARCELS_DATA_SF_VIEW_ID = 'acdm-wktn'
