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

If you want to use the latest available data to create a schemaless file and
have an internet connection, just run

```sh
pipenv shell

python3 -m schemaless.create_schemaless \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  schemaless-one.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=outputdata/likelies-one.csv \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  schemaless-one.csv \
  uuid-map-one.csv
```

The source datasets will be downloaded to your machine automatically.

### Diffing

Core to the schemaless generation is diffing against past runs. This allows
us to track changes over time, even if the source data only provides a current
snapshot of information. Except for the first run of `create_schemaless`, you
will always diff against a prior version.

```sh
pipenv shell

python3 -m schemaless.create_schemaless \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  --diff schemaless-one.csv \
  schemaless-two.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=likelies-one.csv \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  --uuid_map_file=uuid-map-one.csv \  # Use IDs generated last time
  schemaless-two.csv \
  uuid-map-two.csv
```

Running this immediately after creating the schemaless file for the first time
should not produce a diff. We've included sample data files in the repo to
illustrate diffing.

```sh
pipenv shell

# Note --the-date=2020-02-27, to simulate running this on that date
python3 -m schemaless.create_schemaless \
  --no_download True \
  --planning_file data/planning/planning-2020-03-02.csv.xz \
  --pts_file data/pts/2019-10-31-pts-after-2013.csv.xz \
  --tco_file data/data/tco-2020-01-30.csv \
  --mohcd_pipeline_file data/mohcd/mohcd-pipeline-2020-01-30.csv \
  --mohcd_inclusionary_file data/mohcd/mohcd-inclusionary-2020-02-05.csv \
  --permit_addenda data/pts/2019-12-18-permit-addenda.csv.xz \
  --affordable_file data/mohcd/affordable-rental-portfolio-2019-09-06.csv \
  --oewd_permits_file data/oewd-permits-2020-03-03.csv \
  --the-date=2020-02-27 \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  schemaless-one.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=likelies-one.csv \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  schemaless-one.csv \
  uuid-map-one.csv

# Note --the-date=2020-03-04, and that we don't need to specify every data
# source file
python3 -m schemaless.create_schemaless \
  --no_download True \  # Don't download anything not passed in
  --planning_file data/planning/planning-2020-03-11.csv.xz \
  --pts_file data/pts/2020-01-24-pts-after-2013.csv.xz \
  --the-date=2020-03-04 \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  --diff schemaless-one.csv \
  schemaless-two.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=likelies-two.csv \
  --uuid_map_file=uuid-map-one.csv \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  schemaless-two.csv \
  uuid-map-two.csv
```

**Note** This also produces the file `likelies-two.csv` which can be used to determine
possible matches and modify upstream data sources.

### Running relational scripts

After producing the schemaless csv's, you can produce the relational csv's by running
the commands below and using the latest version of the schemaless csv's that have
been produced:

```sh
python3 -m relational.process_schemaless \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  schemaless-two.csv \
  uuid-map-two.csv
```
