#!/usr/bin/env bash

curl -o buffalo_acs_tracts.csv \
  'https://api.census.gov/data/2023/acs/acs5?get=NAME,B02001_001E,B25001_001E&for=tract:*&in=state:36%20county:029'
