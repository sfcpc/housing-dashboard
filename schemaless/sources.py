# Lint as: python3
"""Source class to define the interface for reading a source file."""
from csv import DictReader
from datetime import datetime
from fileutils import open_file

from scourgify.exceptions import AddressNormalizationError
from scourgify.normalize import format_address_record
from scourgify.normalize import normalize_address_record


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
                vals.append(record.get(field))
        return "_".join([self.prefix] + vals)


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
            print("WARN1: Unparseable: %s" % addr_str)
            return ""
        else:
            if not addr['postal_code']:
                # We need this to normalize again, and we can't guess it
                try:
                    return format_address_record(addr)
                except AddressNormalizationError:
                    print("WARN2: Unable to format address %s" % addr)
                    return ""

            if not addr['city']:
                addr['city'] = "San Francisco"
            if not addr['state']:
                addr['state'] = "California"

        try:
            addr = normalize_address_record(addr)
        except AddressNormalizationError:
            print("WARN3: Unparseable: %s" % addr)
            return ""
        else:
            try:
                return format_address_record(addr)
            except AddressNormalizationError:
                print("WARN4: Unable to parse address %s" % addr_str)
                print(addr)
                return ""
        return ""


class Source:
    NAME = 'Base Class'
    FK = PrimaryKey(NAME, 'None')
    DATE = Date('None', '%m/%d/%Y')
    FIELDS = {}

    def __init__(self, filepath):
        self._filepath = filepath

    def foreign_key(self, record):
        return self.FK.get_value(record)

    def yield_records(self):
        with open_file(self._filepath,
                       mode='rt',
                       encoding='utf-8',
                       errors='replace') as inf:
            reader = DictReader(inf)

            for line in reader:
                calc_fields = {}
                ret = {}
                for line_key, ret_key in self.FIELDS.items():
                    # save calculated Fields for later
                    if isinstance(ret_key, Field):
                        calc_fields[line_key] = ret_key
                        continue

                    if line_key not in line:
                        continue

                    val = (line.get(line_key) or "").strip()
                    if val:
                        ret[ret_key] = val

                for key, field in calc_fields.items():
                    val = field.get_value_str(ret)
                    if val:
                        ret[key] = val.strip()
                yield ret


class PPTS(Source):
    NAME = 'ppts'
    FK = PrimaryKey(NAME, 'record_id')
    DATE = Date('date_opened', '%m/%d/%Y')
    OUTPUT_NAME = 'planning'
    FIELDS = {
        'record_id': 'record_id',
        'record_type': 'record_type',
        'record_type_category': 'record_type_category',
        'record_name': 'name',
        'description': 'description',
        'parent': 'parent',
        'children': 'children',
        'record_status': 'status',
        'date_opened': 'date_opened',
        'date_closed': 'date_closed',
        # Location details
        'address': 'address',
        'address_norm': Address('address'),
        'the_geom': 'the_geom',

        # Developer and Planner
        'developer_name': 'TODO',
        'planner_name': 'planner_name',
        'planner_email': 'planner_email',
        'planner_phone': 'planner_phone',

        # Child record details
        'incentives': 'TODO',
        'ppa_submitted': 'TODO',
        'ppa_letter_issued': 'TODO',
        'prj_submitted': 'TODO',
        'nia_issued': 'TODO',
        'application_accepted': 'TODO',
        'pcl_issued': 'TODO',
        'project_desc_stable': 'TODO',
        'env_review_type': 'TODO',
        'first_hearing': 'TODO',
        'final_hearing': 'TODO',
        'entitlements_issued': 'TODO',

        # Unit/land use details
        'non_housing_uses': 'TODO',
        'RELATED_BUILDING_PERMIT': 'building_permit_number',
        'LAND_USE_RESIDENTIAL_EXIST': 'residential_sq_ft_existing',
        'LAND_USE_RESIDENTIAL_PROP': 'residential_sq_ft_proposed',
        'LAND_USE_RESIDENTIAL_NET': 'residential_sq_ft_net',
        'ADU': 'is_adu',  # TOOD: Normalize this bool? True == "CHECKED"
        'PRJ_FEATURE_AFFORDABLE_EXIST': 'affordable_units_existing',
        'PRJ_FEATURE_AFFORDABLE_PROP': 'affordable_units_proposed',
        'PRJ_FEATURE_AFFORDABLE_NET': 'affordable_units_net',
        'PRJ_FEATURE_MARKET_RATE_EXIST': 'market_rate_units_existing',
        'PRJ_FEATURE_MARKET_RATE_PROP': 'market_rate_units_proposed',
        'PRJ_FEATURE_MARKET_RATE_NET': 'market_rate_units_net',
        'PRJ_FEATURE_PARKING_EXIST': 'parking_sq_ft_exist',
        'PRJ_FEATURE_PARKING_PROP': 'parking_sq_ft_proposed',
        'PRJ_FEATURE_PARKING_NET': 'parking_sq_ft_net',
        'RESIDENTIAL_STUDIO_EXIST': 'residential_units_studio_existing',
        'RESIDENTIAL_STUDIO_PROP': 'residential_units_studio_proposed',
        'RESIDENTIAL_STUDIO_NET': 'residential_units_studio_net',
        'RESIDENTIAL_1BR_EXIST': 'residential_units_1br_existing',
        'RESIDENTIAL_1BR_PROP': 'residential_units_1br_proposed',
        'RESIDENTIAL_1BR_NET': 'residential_units_1br_net',
        'RESIDENTIAL_2BR_EXIST': 'residential_units_2br_existing',
        'RESIDENTIAL_2BR_PROP': 'residential_units_2br_proposed',
        'RESIDENTIAL_2BR_NET': 'residential_units_2br_net',
        'RESIDENTIAL_3BR_EXIST': 'residential_units_3br_existing',
        'RESIDENTIAL_3BR_PROP': 'residential_units_3br_proposed',
        'RESIDENTIAL_3BR_NET': 'residential_units_3br_net',
        'RESIDENTIAL_ADU_STUDIO_EXIST':
        'residential_units_adu_studio_existing',  # NOQA
        'RESIDENTIAL_ADU_STUDIO_PROP': 'residential_units_adu_studio_proposed',
        'RESIDENTIAL_ADU_STUDIO_NET': 'residential_units_adu_studio_net',
        'RESIDENTIAL_ADU_STUDIO_AREA': 'residential_sq_ft_adu_studio',
        'RESIDENTIAL_ADU_1BR_EXIST': 'residential_units_adu_1br_existing',
        'RESIDENTIAL_ADU_1BR_PROP': 'residential_units_adu_1br_proposed',
        'RESIDENTIAL_ADU_1BR_NET': 'residential_units_adu_1br_net',
        'RESIDENTIAL_ADU_1BR_AREA': 'residential_sq_ft_adu_1br',
        'RESIDENTIAL_ADU_2BR_EXIST': 'residential_units_adu_2br_existing',
        'RESIDENTIAL_ADU_2BR_PROP': 'residential_units_adu_2br_proposed',
        'RESIDENTIAL_ADU_2BR_NET': 'residential_units_adu_2br_net',
        'RESIDENTIAL_ADU_2BR_AREA': 'residential_sq_ft_adu_2br',
        'RESIDENTIAL_ADU_3BR_EXIST': 'residential_units_adu_3br_existing',
        'RESIDENTIAL_ADU_3BR_PROP': 'residential_units_adu_3br_proposed',
        'RESIDENTIAL_ADU_3BR_NET': 'residential_units_adu_3br_net',
        'RESIDENTIAL_ADU_3BR_AREA': 'residential_sq_ft_adu_3br',
        'RESIDENTIAL_SRO_EXIST': 'residential_units_sro_existing',
        'RESIDENTIAL_SRO_PROP': 'residential_units_sro_proposed',
        'RESIDENTIAL_SRO_NET': 'residential_units_sro_net',
        'RESIDENTIAL_MICRO_EXIST': 'residential_units_micro_existing',
        'RESIDENTIAL_MICRO_PROP': 'residential_units_micro_proposed',
        'RESIDENTIAL_MICRO_NET': 'residential_units_micro_net',
    }
    DATA_SF = "https://data.sfgov.org/dataset/PPTS-Records_data/kgai-svwy"


class PTS(Source):
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
        'address_norm': Address(
            'street_number',
            'street_number_suffix',
            'street_name',
            'street_name_suffix',
            'unit',
            'unit_suffix',
            'zipcode',
        ),
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
    }
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9"  # NOQA


class TCO(Source):
    NAME = 'tco'
    DATE = Date('date_issued', '%Y/%m/%d')
    FK = PrimaryKey(NAME, 'building_permit_number', DATE)
    FIELDS = {
        'Building Permit Application Number': 'building_permit_number',
        'Building Address': 'address',
        'address_norm': Address('address'),
        'Date Issued': 'date_issued',
        'Document Type': 'building_permit_type',
        'Number of Units Certified': 'num_units',
    }
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Dwelling-Unit-Completion-Counts-by-Building-Permit/j67f-aayr"  # NOQA


class MOHCDInclusionary(Source):
    NAME = 'mohcd_inclusionary'
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
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Residential-Projects-With-Inclusionary-Requirement/nj3x-rw36"  # NOQA


class MOHCDPipeline(Source):
    NAME = 'mohcd_pipeline'
    FK = PrimaryKey(NAME, 'project_id')
    FIELDS = {
        'Project ID': 'project_id',
        'Project Status': 'project_status',
        'Project Name': 'project_name',
        'Street Number': 'street_number',
        'Street Name': 'street_name',
        'Street Type': 'street_type',
        'Zip Code': 'zip_code',
        'address_norm': Address(
            'street_number',
            'street_name',
            'street_type',
            'zip_code',
        ),
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
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Affordable-Housing-Pipeline/aaxw-2cb8"  # NOQA


class AffordableRentalPortfolio(Source):
    """MOHCD/OCII's affordable rental portfolio"""
    NAME = 'bmr'
    DATE = Date('year_affordability_began', '%Y')
    FK = PrimaryKey(NAME, 'project_id')
    FIELDS = {
        'Project ID': 'project_id',
        'Project Name': 'project_name',
        'Street Number': 'street_number',
        'Street Name': 'street_name',
        'Street Type': 'street_type',
        'Zip Code': 'zip_code',
        'address_norm': Address(
            'street_number',
            'street_name',
            'street_type',
            'zip_code',
        ),
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
    DATA_SF = "https://data.sfgov.org/Housing-and-Buildings/Mayor-s-Office-of-Housing-and-Community-Developmen/9rdx-httc"  # NOQA


source_map = {
    PPTS.NAME: PPTS,
    PTS.NAME: PTS,
    TCO.NAME: TCO,
    MOHCDPipeline.NAME: MOHCDPipeline,
    MOHCDInclusionary.NAME: MOHCDInclusionary,
    AffordableRentalPortfolio.NAME: AffordableRentalPortfolio,
}
