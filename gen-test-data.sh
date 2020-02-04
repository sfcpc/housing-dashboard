#!/bin/bash
xzcat data/ppts/2018-04-24-ppts.csv.xz | head -n1 > testdata/ppts-one.csv
xzcat data/ppts/2018-04-24-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)|(2015-011205)|(201404304554)|(201705237369)" >> testdata/ppts-one.csv

xzcat data/ppts/2019-06-02-ppts.csv.xz | head -n1 > testdata/ppts-two.csv
xzcat data/ppts/2019-06-02-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)|(2015-011205)|(201404304554)|(201705237369)" >> testdata/ppts-two.csv

head -n1 data/mohcd/mohcd-2020-01-30.csv > testdata/mohcd.csv
cat data/mohcd/mohcd-2020-01-30.csv | egrep -i "(1950 mission)|(2015-011205)|(transbay)" >> testdata/mohcd.csv

xzcat data/pts/2020-01-24-pts-after-2013.csv.xz | head -n1 > testdata/pts.csv 
xzcat data/pts/2020-01-24-pts-after-2013.csv.xz | egrep -i "(201705318009)|(201706058373)|(201711284964)|(201801118389)|(201404304554)|(201705237369)" >> testdata/pts.csv

python3 -m schemaless.create_schemaless \
  --ppts_file testdata/ppts-one.csv \
  --pts_file testdata/pts.csv \
  --tco_file testdata/tco.csv \
  --mohcd_file testdata/mohcd.csv \
  --out_file testdata/schemaless-one.csv \
  --the-date=2020-01-29
# We read in the uuid-map file generated previously so our uuids are stable
python3 -m schemaless.create_uuid_map \
  testdata/schemaless-one.csv \
  testdata/uuid-map-one.csv \
  --uuid_map_file=testdata/uuid-map-one.csv
python3 -m schemaless.create_schemaless \
  --ppts_file testdata/ppts-two.csv \
  --out_file testdata/schemaless-two.csv \
  --diff testdata/schemaless-one.csv \
  --the-date=2020-01-29
# We read in the uuid-map file generated previously so our uuids are stable
python3 -m schemaless.create_uuid_map \
  testdata/schemaless-one.csv \
  testdata/uuid-map-two.csv \
  --uuid_map_file=testdata/uuid-map-two.csv
