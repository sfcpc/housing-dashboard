# housing-dashboard

```sh
pipenv shell
python3 create_schemaless.py data/ppts/2018-04-24-ppts.csv.xz
```

## Development

### Set up your development environment

```sh
git clone https://github.com/sbuss/housing-dashboard.git
cd housing-dashboard
pipenv sync --dev
```

### Run tests

We use [pytest](https://docs.pytest.org/) for our tests. To run the tests, just
run:

```sh
pytest
```
