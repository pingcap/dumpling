#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

run_sql "drop database if exists primary_key"
run_sql "create database primary_key"
export DUMPLING_TEST_DATABASE=primary_key

for data in "$DUMPLING_BASE_NAME"/data/*; do
  run_sql_file "$data"
done

run_dumpling

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  table_name="${base_name%.sql}"
  file_should_exist "$DUMPLING_BASE_NAME/result/$table_name.sql"
  file_should_exist "$DUMPLING_OUTPUT_DIR/primary_key.$table_name.000000000.sql"
  diff "$DUMPLING_BASE_NAME/result/$table_name.sql" "$DUMPLING_OUTPUT_DIR/primary_key.$table_name.000000000.sql"
done
