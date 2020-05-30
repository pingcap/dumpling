#!/bin/sh

set -eu

db="quo\`te-database"
run_sql "drop database if exists $db"
run_sql "create database $db"
export DUMPLING_TEST_DATABASE=$db

for data in "$DUMPLING_BASE_NAME"/data/*; do
  run_sql_file "$data"
done

run_dumpling

for file_path in "$DUMPLING_BASE_NAME"/data/*; do
  base_name=$(basename "$file_path")
  file_should_exist "$DUMPLING_BASE_NAME/data/$base_name"
  file_should_exist "$DUMPLING_OUTPUT_DIR/$base_name"
  diff "$DUMPLING_BASE_NAME/data/$base_name" "$DUMPLING_OUTPUT_DIR/$base_name"
done
