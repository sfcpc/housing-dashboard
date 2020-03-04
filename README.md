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

We use data that is downloaded from DataSF. The different data sources that need
be used for the schemaless are below:

1. [SF Planning Permitting Data from Planning](https://data.sfgov.org/Housing-and-Buildings/SF-Planning-Permitting-Data/kncr-c6jw)
1. [Building Permits from DBI](https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9)
1. [Dwelling Unit Completion Counts by Building Permit from DBI](https://data.sfgov.org/Housing-and-Buildings/Dwelling-Unit-Completion-Counts-by-Building-Permit/j67f-aayr)
1. [Department of Building Inspection Permit Addenda with Routing from DBI](https://data.sfgov.org/Housing-and-Buildings/Department-of-Building-Inspection-Permit-Addenda-w/87xy-gk8d)
1. [Affordable Housing Pipeline from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Affordable-Housing-Pipeline/aaxw-2cb8)
1. [Residential Projects with Inclusionary Requirements from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Residential-Projects-With-Inclusionary-Requirement/nj3x-rw36)
1. [Mayor's Office of Housing and Community Development Affordable Rental Portfolio from MOHCD](https://data.sfgov.org/Housing-and-Buildings/Mayor-s-Office-of-Housing-and-Community-Developmen/9rdx-httc)
1. [Parcels - Active and Retired from CCSF](https://data.sfgov.org/Geographic-Locations-and-Boundaries/Parcels-Active-and-Retired/acdm-wktn)
1. [Priority Permits from OEWD](https://data.sfgov.org/dataset/Priority-Permits/336t-bzzm)

```sh
pipenv shell
python3 create_schemaless.py data/ppts/2018-04-24-ppts.csv.xz
```

