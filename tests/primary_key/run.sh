#!/bin/sh

set -eu

run_sql "drop database if exists primary_key"
run_sql "create database primary_key"
export DUMPLING_TEST_DATABASE=primary_key

run_sql_file "$DUMPLING_BASE_NAME/data/test_case_1.sql"
run_dumpling

file_should_exist "$DUMPLING_OUTPUT_DIR/primary_key.t.sql"
file_should_exist "$DUMPLING_BASE_NAME/result/test_case_1.sql"
diff "$DUMPLING_OUTPUT_DIR/primary_key.t.sql" "$DUMPLING_BASE_NAME/result/test_case_1.sql"
