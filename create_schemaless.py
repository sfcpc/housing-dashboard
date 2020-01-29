# Lint as: python3
"""Convert departmental data files into a schemaless csv.

If you run this with a PPTS and PTS file specified, it will
dump those into a schemaless csv. If you provide the files
and also set the --diff flag, it will diff against an existing
schemaless csv.
"""

import argparse
from collections import defaultdict
import csv
from csv import DictReader
from datetime import date
import shutil
import sys

from fileutils import open_file

csv.field_size_limit(sys.maxsize)

# Names of departmental data sources.
PPTS = 'ppts'
PTS = 'pts'

foreign_keys = {PPTS: 'record_id', PTS: 'Record ID'}

fields = {
    PPTS: {
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
        'RELATED_BUILDING_PERMIT': 'building_permit_id',
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
    },
    PTS: {
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
    }
}


def just_dump(sources, outfile):
    with open(outfile, 'w') as outf:
        writer = csv.writer(outf)
        writer.writerow(['fk', 'source', 'last_updated', 'name', 'value'])
        last_updated = date.today().isoformat()

        for source_name, source_file in sources.items():
            with open_file(source_file,
                           mode='rt',
                           encoding='utf-8',
                           errors='replace') as inf:
                reader = DictReader(inf)

                for line in reader:
                    fk = line[foreign_keys[source_name]]

                    for (key, val) in line.items():
                        if key not in fields[source_name]:
                            continue
                        if val:
                            writer.writerow([
                                fk, source_name, last_updated,
                                fields[source_name][key], val
                            ])


def latest_values(schemaless_file):
    """Collapse the schemaless file into the latest values for each record."""
    records = defaultdict(lambda: defaultdict(str))
    with open(schemaless_file, 'r') as inf:
        reader = DictReader(inf)
        for line in reader:
            records[line['fk']][line['name']] = line['value']
    return records


def dump_and_diff(sources, outfile, schemaless_file):
    records = latest_values(schemaless_file)
    print("Loaded %d records" % len(records))

    shutil.copyfile(schemaless_file, outfile)
    with open(outfile, 'a') as outf:
        writer = csv.writer(outf)
        last_updated = date.today().isoformat()

        for source_name, source_file in sources.items():
            with open_file(source_file,
                           mode='rt',
                           encoding='utf-8',
                           errors='replace') as inf:
                reader = DictReader(inf)

                for line in reader:
                    fk = line[foreign_keys[source_name]]
                    for (key, val) in line.items():
                        if key not in fields[source_name]:
                            continue
                        if val and val != records[fk][key]:
                            records[fk][key] = val
                            writer.writerow([
                                fk, source_name, last_updated,
                                fields[source_name][key], val
                            ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('ppts_file', help='PPTS file')
    parser.add_argument('pts_file', help='PTS file')
    parser.add_argument('out_file', help='output file for schemaless csv')

    parser.add_argument(
        '--diff',
        help='A schemaless csv generated by this script, to diff against.',
        default='')
    args = parser.parse_args()

    source_map = {PPTS: args.ppts_file, PTS: args.pts_file}

    if not args.diff:
        just_dump(source_map, args.out_file)
    else:
        dump_and_diff(source_map, args.out_file, args.diff)
