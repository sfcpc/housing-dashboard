# housing-dashboard

## Development

### Set up your development environment

```sh
git clone https://github.com/sfcpc/housing-dashboard.git
cd housing-dashboard
pipenv sync --dev
```
### Formatting

We use [flake8](https://flake8.pycqa.org/en/latest/) for style guide
enforcement. To run the linter, just run:

```sh
flake8
```

### Run tests

We use [pytest](https://docs.pytest.org/) for our tests. To run the tests, just
run:

```sh
pytest
```

## Running locally

### Downloading data

NOTE: You do not need to manually download this data. By default (unless you
pass `--no_download True` to `create_schemaless.py`) the script will download
the latest version of every file.

If you would prefer to download the files to speed up testing, you can find
the various data sources below:

1. [SF Planning Permitting Data from Planning](https://data.sfgov.org/Housing-and-Buildings/SF-Planning-Permitting-Data/kncr-c6jw)
1. [Building Permits from DBI](https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9)
1. [Dwelling Unit Completion Counts by Building Permit from DBI](https://data.sfgov.org/Housing-and-Buildings/Dwelling-Unit-Completion-Counts-by-Building-Permit/j67f-aayr)
1. [Department of Building Inspection Permit Addenda with Routing from DBI](https://data.sfgov.org/Housing-and-Buildings/Department-of-Building-Inspection-Permit-Addenda-w/87xy-gk8d)
1. [Affordable Housing Pipeline from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Affordable-Housing-Pipeline/aaxw-2cb8)
1. [Residential Projects with Inclusionary Requirements from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Residential-Projects-With-Inclusionary-Requirement/nj3x-rw36)
1. [Mayor's Office of Housing and Community Development Affordable Rental Portfolio from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Mayor-s-Office-of-Housing-and-Community-Developmen/9rdx-httc)
1. [Parcels - Active and Retired from CCSF](https://data.sfgov.org/Geographic-Locations-and-Boundaries/Parcels-Active-and-Retired/acdm-wktn)
1. [Priority Permits from OEWD](https://data.sfgov.org/dataset/Priority-Permits/336t-bzzm)

### Running schemaless scripts

Once CSV's from all of the data sources above have been downloaded, you must produce
a `schemalesss` and `uuid_map` file. Below is an example of running these commands once on
`02-27-2020` and then running it again to grab diffs on `03-04-2020`

```sh
pipenv shell

python3 -m schemaless.create_schemaless \
--planning_file inputdata/2020-02-27-planning.csv \
--pts_file inputdata/2020-02-27-pts-after-2013.csv \
--tco_file inputdata/2020-02-27-tco.csv \
--mohcd_pipeline_file inputdata/2020-02-27-mohcd-pipeline.csv \
--mohcd_inclusionary_file inputdata/2020-02-27-mohcd-inclusionary.csv \
--permit_addenda inputdata/2020-02-27-permit-addenda.csv \
--affordable_file inputdata/2020-02-27-affordable-rental-portfolio.csv \
--oewd_permits_file inputdata/2020-02-27-oewd-permits.csv \
--the-date=2020-02-27 \
--parcel_data_file=inputdata/2020-02-27-parcels.csv \
outputdata/schemaless-one.csv

python3 -m schemaless.create_uuid_map \
outputdata/schemaless-one.csv \
outputdata/uuid-map-one.csv \
--likely_match_file=outputdata/likelies-one.csv \
--parcel_data_file=inputdata/2020-02-27-parcels.csv
```

The above was a run of the schemaless scripts for `02-27-2020`, now if we've downloaded
updated CSV's for `03-04-2020`, we can run the scripts again to get any new data

```sh
python3 -m schemaless.create_schemaless \
--planning_file inputdata/2020-03-04-planning.csv \
--pts_file inputdata/2020-03-04-pts-after-2013.csv \
--tco_file inputdata/2020-03-04-tco.csv \
--mohcd_pipeline_file inputdata/2020-03-04-mohcd-pipeline.csv \
--mohcd_inclusionary_file inputdata/2020-03-04-mohcd-inclusionary.csv \
--permit_addenda inputdata/2020-03-04-permit-addenda.csv \
--affordable_file inputdata/2020-03-04-affordable-rental-portfolio.csv \
--oewd_permits_file inputdata/2020-03-04-oewd-permits.csv \
--the-date=2020-03-04 \
--parcel_data_file=inputdata/2020-03-04-parcels.csv \
--diff outputdata/schemaless-one.csv \
outputdata/schemaless-two.csv

python3 -m schemaless.create_uuid_map \
outputdata/schemaless-two.csv \
outputdata/uuid-map-two.csv \
--likely_match_file=outputdata/likelies-two.csv \
--uuid_map_file=testdata/uuid-map-one.csv \
--parcel_data_file=inputdata/2020-03-04-parcels.csv
```

**Note** This also produces the file `likelies-two.csv` which can be used to determine
possible matches and modify upstream data sources.

### Running relational scripts

After producing the schemaless csv's, you can produce the relational csv's by running
the commands below and using the latest version of the schemaless csv's that have
been produced:

```sh
python3 -m relational.process_schemaless \
outputdata/schemaless-two.csv \
outputdata/uuid-map-two.csv \
--parcel_data_file=inputdata/2020-03-04-parcels.csv
```
