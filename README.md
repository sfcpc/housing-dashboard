# SF Planning Housing Production Dashboard

This repo contains scripts to transform assorted datasets from DataSF into a
single set of data for the new San Francisco Housing Production Dashboard.

<!-- vim-markdown-toc GFM -->

* [Overview](#overview)
  * [Schemaless](#schemaless)
  * [UUIDs](#uuids)
  * [Relational tables](#relational-tables)
* [Development](#development)
  * [Set up your development environment](#set-up-your-development-environment)
  * [Formatting](#formatting)
  * [Run tests](#run-tests)
* [Running locally](#running-locally)
  * [Using Airflow](#using-airflow)
    * [Set Up Airflow](#set-up-airflow)
    * [Running Tasks](#running-tasks)
    * [Testing DataSF Uploads](#testing-datasf-uploads)
  * [Running manually](#running-manually)
    * [Generating schemaless files](#generating-schemaless-files)
    * [Diffing](#diffing)
    * [Generating relational tables](#generating-relational-tables)
  * [Downloading data](#downloading-data)
* [Troubleshooting](#troubleshooting)
  * [I added a column to a csv, but it isn't showing up in the DataSF dataset](#i-added-a-column-to-a-csv-but-it-isnt-showing-up-in-the-datasf-dataset)
  * [I can't download from or upload data to DataSF](#i-cant-download-from-or-upload-data-to-datasf)

<!-- vim-markdown-toc -->

## Overview

There are three primary scripts:

1. [`schemaless/create_schemaless.py`](schemaless/create_schemaless.py)
1. [`schemaless/create_uuid_map.py`](schemaless/create_uuid_map.py)
1. [`relational/process_schemaless.py`](relational/process_schemaless.py)

They each feed into the next and produce useful outputs.

### Schemaless

The schemaless file is a key/value dataset that combines data from various
different data [sources](schemaless/sources.py). The goal is to merge data into
a single file, with a few minor transformations to standardize a few things. The
output of `schemaless/create_schemaless.py` is a csv with just five columns:

1. Foreign key: The key for this record
1. Data source: The name of the source that provided the data
1. Last updated date: Date this row was written to the file.
1. Key: The name of the value being populated
1. Value: The actual value

The file ends up looking like this:

```csv
fk,source,last_updated,name,value
planning_09-053,planning,01/29/2020 12:00:00 AM,record_id,09-053
planning_09-053,planning,01/29/2020 12:00:00 AM,record_type,APL
planning_09-053,planning,01/29/2020 12:00:00 AM,status,Closed - Appeal Denied
pts_129220091307,pts,01/29/2020 12:00:00 AM,permit_number,201301027105
pts_129220091307,pts,01/29/2020 12:00:00 AM,permit_type,8
pts_129220091307,pts,01/29/2020 12:00:00 AM,current_status,cancelled
pts_129220091307,pts,01/29/2020 12:00:00 AM,current_status_date,11/14/2016
pts_129220091307,pts,01/29/2020 12:00:00 AM,filed_date,01/02/2013
tco_201404304554_2018-01-11,tco,01/29/2020 12:00:00 AM,building_permit_number,201404304554
tco_201404304554_2018-01-11,tco,01/29/2020 12:00:00 AM,address,41 Tehama Street
tco_201404304554_2018-01-11,tco,01/29/2020 12:00:00 AM,date_issued,2018/01/11
tco_201404304554_2018-01-11,tco,01/29/2020 12:00:00 AM,building_permit_type,Amended TCO
tco_201404304554_2018-01-11,tco,01/29/2020 12:00:00 AM,num_units,68
mohcd_pipeline_2016-023,mohcd_pipeline,01/30/2020 12:00:00 AM,project_id,2016-023
mohcd_pipeline_2016-023,mohcd_pipeline,01/30/2020 12:00:00 AM,project_status,(6) Complete
mohcd_pipeline_2016-023,mohcd_pipeline,01/30/2020 12:00:00 AM,total_project_units,160
mohcd_pipeline_2016-023,mohcd_pipeline,01/30/2020 12:00:00 AM,total_affordable_units,19
mohcd_pipeline_2016-023,mohcd_pipeline,01/30/2020 12:00:00 AM,total_market_rate_units,141
mohcd_pipeline_2016-023,mohcd_pipeline,01/30/2020 12:00:00 AM,percent_affordable,12
bmr_2003-030,bmr,02/15/2020 12:00:00 AM,project_id,2003-030
bmr_2003-030,bmr,02/15/2020 12:00:00 AM,project_name,Valencia Gardens
bmr_2003-030,bmr,02/15/2020 12:00:00 AM,project_sponsor,Mission Housing Development Corporation
bmr_2003-030,bmr,02/15/2020 12:00:00 AM,total_units,260
bmr_2003-030,bmr,02/15/2020 12:00:00 AM,total_affordable_units,259
...
```

Because this is an append-only file, it contains a full history of housing data
even if the source data is just a snapshot. Though, note, that because this
project was started in late 2019, we do not necessarily have a full history of
projects that were started or completed prior to this date.

### UUIDs

The UUID file is used to provide an identifier which can link together records
across data sources. This allows us to follow a project as it moves between
Planning, DBI, or other departments. For example, consider 1950 Mission St. In
the snippet below it has a record ID of `planning_2016-001514PRJ`, and we
assigned it the UUID `17083f61-9ce4-4972-96f2-5ecf9133286d`. This same UUID is
used for the records `pts_1438278158065` (from the Department of Building
Inspection) and `mohcd_pipeline_2013-046` (from the Mayor's Office of Housing
and Community Development).

```csv
uuid,fk
58106875-57c9-4dd3-b13c-096f6c4153d5,planning_09-053
17083f61-9ce4-4972-96f2-5ecf9133286d,planning_2016-001514PRJ
17083f61-9ce4-4972-96f2-5ecf9133286d,pts_1438278158065
ff086391-61c8-4817-a20b-7f4f0131dd7c,pts_129220091307
9b4b24c9-cf10-446f-a653-71fb8ce1bb1d,tco_201404304554_2018-01-11
ac5f0e52-4b25-432a-83f7-59e759841a32,mohcd_pipeline_2016-023
17083f61-9ce4-4972-96f2-5ecf9133286d,mohcd_pipeline_2013-046
2c7cfde0-8dde-47ad-b639-3615e59ded5a,bmr_2003-030
...
```

Calculating which records are related is fairly complex and the logic can be
found in [`schemaless/create_uuid_map.py`](schemaless/create_uuid_map.py). The
important takeaway is that projects which can be linked across datasets share
the same ID.


### Relational tables

The schemaless and UUID files are used to build several smaller csvs which are
used in the PowerBI dashbaord. These relational files collapse all of the
information in schemaless.csv into smaller and more easily understandable files.

## Development

### Set up your development environment

```sh
git clone https://github.com/sfcpc/housing-dashboard.git
cd housing-dashboard
pipenv sync --dev
pipenv shell
```

**NOTE**: All commands in the README should be run in a `pipenv shell`.

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

### Using Airflow

Airflow is the preferred way to run the scripts in this repo.

#### Set Up Airflow

To set up airflow, run:

```sh
# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=./.airflow

# initialize the database
airflow initdb

# Set variables for our dag
airflow variables --set WORKDIR "$(pwd)/airflow"
airflow variables --set UPLOAD False  # Disable uploading to DataSF
mkdir -p $(pwd)/airflow
```

#### Running Tasks

To run the tasks, you can either start the webserver and scheduler and
let it go, or you can run manually via the CLI. For local development,
running manually is likely what you want:

```sh
# Running manually
airflow test housing-dashboard-data create_schemaless $(date "+%Y-%m-%d")
airflow test housing-dashboard-data create_uuid_map $(date "+%Y-%m-%d")
airflow test housing-dashboard-data create_relational $(date "+%Y-%m-%d")
```

You can trigger runs in the web interface, too:

```sh
# start the web server, default port is 8080
airflow webserver -p 8080

# In a new terminal, start the scheduler
airflow scheduler
```

Then visit http://localhost:8080 and enable the housing-dashboard DAG.

#### Testing DataSF Uploads

Enable uploading in the airflow pipeline by setting `UPLOAD` to `True` and
providing your DataSF credentials.

```sh
airflow variables --set DATASF_USER foo
airflow variables --set DATASF_PASS bar
airflow variables --set UPLOAD True
```

### Running manually

While airflow is preferred, you may also run each script individually and
provide whatever input data you'd like. You would only do this is you want to
iterate quickly on a change to one of the scripts and don't need to test the
entire workflow.

#### Generating schemaless files

If you want to use the latest available data to create a schemaless file and
have an internet connection, just run

```sh
pipenv shell

python3 -m schemaless.create_schemaless \
  schemaless-one.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=outputdata/likelies-one.csv \
  schemaless-one.csv \
  uuid-map-one.csv
```

The source datasets will be downloaded to your machine automatically.

#### Diffing

Core to the schemaless generation is diffing against past runs. This allows
us to track changes over time, even if the source data only provides a current
snapshot of information. Except for the first run of `create_schemaless`, you
will always diff against a prior version.

```sh
pipenv shell

python3 -m schemaless.create_schemaless \
  --diff True \  # The prior schemaless file will be automatically downloaded
  schemaless-two.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=likelies-one.csv \
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
  schemaless-one.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=likelies-one.csv \
  schemaless-one.csv \
  uuid-map-one.csv

# Note --the-date=2020-03-04, and that we don't need to specify every data
# source file
python3 -m schemaless.create_schemaless \
  --no_download True \  # Don't download anything not passed in
  --planning_file data/planning/planning-2020-03-11.csv.xz \
  --pts_file data/pts/2020-01-24-pts-after-2013.csv.xz \
  --the-date=2020-03-04 \
  --diff_file schemaless-one.csv \
  schemaless-two.csv

python3 -m schemaless.create_uuid_map \
  --likely_match_file=likelies-two.csv \
  --uuid_map_file=uuid-map-one.csv \
  schemaless-two.csv \
  uuid-map-two.csv
```

**NOTE** This also produces the file `likelies-two.csv` which can be used to determine
possible matches and modify upstream data sources.

#### Generating relational tables

The "relational" tables are produced from the "schemaless" files to make
visualization and analysis easier. You can produce these files by running:

```sh
python3 -m relational.process_schemaless \
  schemaless-two.csv \
  uuid-map-two.csv
```

### Downloading data

**NOTE**: By default the scripts will download the latest version of every file
(unless you pass `--no_download True`).

If you would prefer to download the input files to speed up testing, you can
find the various data sources below:

1. [SF Planning Permitting Data from Planning](https://data.sfgov.org/Housing-and-Buildings/SF-Planning-Permitting-Data/kncr-c6jw)
1. [Building Permits from DBI](https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9)
1. [Dwelling Unit Completion Counts by Building Permit from DBI](https://data.sfgov.org/Housing-and-Buildings/Dwelling-Unit-Completion-Counts-by-Building-Permit/j67f-aayr)
1. [Department of Building Inspection Permit Addenda with Routing from DBI](https://data.sfgov.org/Housing-and-Buildings/Department-of-Building-Inspection-Permit-Addenda-w/87xy-gk8d)
1. [Affordable Housing Pipeline from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Affordable-Housing-Pipeline/aaxw-2cb8)
1. [Residential Projects with Inclusionary Requirements from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Residential-Projects-With-Inclusionary-Requirement/nj3x-rw36)
1. [Mayor's Office of Housing and Community Development Affordable Rental Portfolio from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Mayor-s-Office-of-Housing-and-Community-Developmen/9rdx-httc)
1. [Parcels - Active and Retired from CCSF](https://data.sfgov.org/Geographic-Locations-and-Boundaries/Parcels-Active-and-Retired/acdm-wktn)
1. [Priority Permits from OEWD](https://data.sfgov.org/dataset/Priority-Permits/336t-bzzm)

## Troubleshooting

### I added a column to a csv, but it isn't showing up in the DataSF dataset

If you add or remove columns, DataSF will silently drop those changes. You will
need to upload the new file and configure the dataset metadata manually through
the socrata web UI.

### I can't download from or upload data to DataSF

Ensure that you've set `DATASF_USER` and `DATASF_PASS` to the correct values.
These can be configure either as environment variables or in your airflow
config.

Also, be sure to set `airflow variables --set UPLOAD True` or pass
`--upload=True` when running manually.
