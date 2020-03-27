#!/bin/bash
set -xe

python gen_test_data.py data/planning/planning-2020-02-27.csv.xz testdata/planning-one.csv "(1950 mission)|(8 10th)|(429 beale)|(430 main)|(2015-011205)|(201404304554)|(201705237369)|(2015-014058PRJ)|((340 Valencia)|(2002.0809))|(2451 Sacramento)|(2017-016047PRJ)|(2017-016045PRJ)"

python gen_test_data.py data/planning/planning-2020-03-02.csv.xz testdata/planning-two.csv "(1950 mission)|(8 10th)|(429 beale)|(430 main)|(2015-011205)|(201404304554)|(201705237369)|(2015-014058PRJ)|((340 Valencia)|(2002.0809))|(2451 Sacramento)|(2017-016047PRJ)|(2017-016045PRJ)"

python gen_test_data.py data/mohcd/mohcd-pipeline-2020-01-30.csv testdata/mohcd-pipeline.csv "(1950 mission)|(2015-011205)|(transbay)|(2011-005)|(2016-023)|(2015-014058PRJ)"

python gen_test_data.py data/mohcd/affordable-rental-portfolio-2019-09-06.csv testdata/affordable-rental-portfolio.csv "(Valencia Gardens)|(Kennedy Towers)"

# Unfortunately, to include every possible match for 2451 Sacramento, we just
# need to search for "Sacramento". That's a lot of non-matches.
python gen_test_data.py data/pts/2020-01-24-pts-after-2013.csv.xz testdata/pts.csv "(201705318009)|(201706058373)|(201711284964)|(201801118389)|(201404304554)|(201705237369)|(201301027105)|(Sacramento)|(201609218371)|(201910225142)|(201910225150)|(201910225151)|(201910225152)|(201910225153)|(201910225154)|(201910225155)|(201712085881)|(201712085886)|(201905170923)|(201905170926)|(201910154483)|(201910154490)|(201910154498)|(201912169614)|(201912169619)"
xzcat data/pts/2020-01-24-pts-after-2013.csv.xz | egrep "(1764,031)" >> testdata/pts.csv


python gen_test_data.py data/mohcd/mohcd-inclusionary-2020-02-05.csv testdata/mohcd-inclusionary.csv "(2011-005)|(2016-023)|(2015-014058PRJ)"

python gen_test_data.py data/pts/2020-02-11-permit-addenda.csv.xz testdata/permit-addenda.csv "(201609218371)|(8410366)|(201810233961)"

python gen_test_data.py data/oewd/oewd-permits-2020-03-03.csv testdata/oewd-permits.csv "(201905170923)|(201905170926)|(201910154483)|(201910154490)|(201910154498)|(201912169614)|(201912169619)"

python3 -m schemaless.create_schemaless \
  --no_download True \
  --planning_file testdata/planning-one.csv \
  --pts_file testdata/pts.csv \
  --tco_file testdata/tco.csv \
  --mohcd_pipeline_file testdata/mohcd-pipeline.csv \
  --mohcd_inclusionary_file testdata/mohcd-inclusionary.csv \
  --permit_addenda testdata/permit-addenda.csv \
  --affordable_file testdata/affordable-rental-portfolio.csv \
  --oewd_permits_file testdata/oewd-permits.csv \
  --the-date=2020-01-29 \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  --diff False \
  --out_file testdata/schemaless-one.csv
# We read in the uuid-map file generated previously so our uuids are stable
python3 -m schemaless.create_uuid_map \
  --no_download True \
  --schemaless_file=testdata/schemaless-one.csv \
  --likely_match_file=testdata/likelies-one.csv \
  --uuid_map_file=testdata/uuid-map-one.csv \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  --out_file testdata/uuid-map-one.csv
python3 -m schemaless.create_schemaless \
  --no_download True \
  --planning_file testdata/planning-two.csv \
  --diff_file testdata/schemaless-one.csv \
  --the-date=2020-01-29 \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  --out_file testdata/schemaless-two.csv
# We read in the uuid-map file generated previously so our uuids are stable
python3 -m schemaless.create_uuid_map \
  --schemaless_file=testdata/schemaless-one.csv \
  --no_download True \
  --likely_match_file=testdata/likelies-two.csv \
  --uuid_map_file=testdata/uuid-map-two.csv \
  --parcel_data_file=data/assessor/2020-02-18-parcels.csv.xz \
  --out_file testdata/uuid-map-two.csv
  # Note: When adding new records, use uuid-map-one so new UUIDs are persisted
  # --uuid_map_file=testdata/uuid-map-one.csv
