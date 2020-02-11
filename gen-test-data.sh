#!/bin/bash
xzcat data/ppts/2018-04-24-ppts.csv.xz | head -n1 > testdata/ppts-one.csv
xzcat data/ppts/2018-04-24-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)|(2015-011205)|(201404304554)|(201705237369)|(2015-014058PRJ)|((340 Valencia)|(2002.0809))|(2451 Sacramento)" >> testdata/ppts-one.csv

xzcat data/ppts/2019-06-02-ppts.csv.xz | head -n1 > testdata/ppts-two.csv
xzcat data/ppts/2019-06-02-ppts.csv.xz | egrep -i "(1950 mission)|(8 10th)|(429 beale)|(430 main)|(2015-011205)|(201404304554)|(201705237369)|(2015-014058PRJ)|((340 Valencia)|(2002.0809))|(2451 Sacramento)" >> testdata/ppts-two.csv

head -n1 data/mohcd/mohcd-pipeline-2020-01-30.csv > testdata/mohcd-pipeline.csv
cat data/mohcd/mohcd-pipeline-2020-01-30.csv | egrep -i "(1950 mission)|(2015-011205)|(transbay)|(2011-005)|(2016-023)|(2015-014058PRJ)" >> testdata/mohcd-pipeline.csv

head -n1 data/mohcd/bmr-2019-09-06.csv > testdata/bmr.csv
cat data/mohcd/bmr-2019-09-06.csv | egrep -i "(Valencia Gardens)|(Kennedy Towers)" >> testdata/bmr.csv

xzcat data/pts/2020-01-24-pts-after-2013.csv.xz | head -n1 > testdata/pts.csv 
xzcat data/pts/2020-01-24-pts-after-2013.csv.xz | egrep -i "(201705318009)|(201706058373)|(201711284964)|(201801118389)|(201404304554)|(201705237369)|(2451,,Sacramento,St)" >> testdata/pts.csv

head -n1 data/mohcd/mohcd-inclusionary-2020-02-05.csv > testdata/mohcd-inclusionary.csv
cat data/mohcd/mohcd-inclusionary-2020-02-05.csv | egrep -i "(2011-005)|(2016-023)|(2015-014058PRJ)" >> testdata/mohcd-inclusionary.csv

python3 -m schemaless.create_schemaless \
  --ppts_file testdata/ppts-one.csv \
  --pts_file testdata/pts.csv \
  --tco_file testdata/tco.csv \
  --mohcd_pipeline_file testdata/mohcd-pipeline.csv \
  --mohcd_inclusionary_file testdata/mohcd-inclusionary.csv \
  --bmr_file testdata/bmr.csv \
  --the-date=2020-01-29
  --the-date=2020-01-29 \
  testdata/schemaless-one.csv
# We read in the uuid-map file generated previously so our uuids are stable
python3 -m schemaless.create_uuid_map \
  testdata/schemaless-one.csv \
  testdata/uuid-map-one.csv \
  --uuid_map_file=testdata/uuid-map-one.csv
python3 -m schemaless.create_schemaless \
  --ppts_file testdata/ppts-two.csv \
  --diff testdata/schemaless-one.csv \
  --the-date=2020-01-29 \
  testdata/schemaless-two.csv
# We read in the uuid-map file generated previously so our uuids are stable
python3 -m schemaless.create_uuid_map \
  testdata/schemaless-one.csv \
  testdata/uuid-map-two.csv \
  --uuid_map_file=testdata/uuid-map-two.csv
  # Note: When adding new records, use uuid-map-one so new UUIDs are persisted
  # --uuid_map_file=testdata/uuid-map-one.csv
