#!/bin/bash
xzcat data/ppts/2018-04-24-ppts.csv.xz | head -n1 > testdata/ppts-one.csv
xzcat data/ppts/2018-04-24-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)" >> testdata/ppts-one.csv

xzcat data/ppts/2019-06-02-ppts.csv.xz | head -n1 > testdata/ppts-two.csv
xzcat data/ppts/2019-06-02-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)" >> testdata/ppts-two.csv

python3 create_schemaless.py testdata/ppts-one.csv testdata/schemaless-one.csv
python3 create_schemaless.py testdata/ppts-two.csv testdata/schemaless-two.csv --diff testdata/schemaless-one.csv