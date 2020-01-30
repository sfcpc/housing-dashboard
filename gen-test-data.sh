#!/bin/bash
xzcat data/ppts/2018-04-24-ppts.csv.xz | head -n1 > testdata/ppts-one.csv
xzcat data/ppts/2018-04-24-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)" >> testdata/ppts-one.csv

xzcat data/ppts/2019-06-02-ppts.csv.xz | head -n1 > testdata/ppts-two.csv
xzcat data/ppts/2019-06-02-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)" >> testdata/ppts-two.csv

python3 create_schemaless.py --ppts_file testdata/ppts-one.csv --out_file testdata/schemaless-one.csv
python3 create_uuid_map.py testdata/schemaless-one.csv testdata/uuid-map-one.csv
python3 create_schemaless.py --ppts_file testdata/ppts-two.csv --out_file testdata/schemaless-two.csv --diff testdata/schemaless-one.csv
python3 create_uuid_map.py testdata/schemaless-one.csv --uuid_map_file=testdata/uuid-map-one.csv testdata/uuid-map-two.csv
