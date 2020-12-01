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

run_sql "DROP DATABASE IF EXISTS naughty_strings"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings-schema-create.sql"
export DUMPLING_TEST_DATABASE="naughty_strings"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.t-schema.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.t.sql"
run_dumpling --escape-backslash=false
# FIXME should compare the schemas too, but they differ too much among MySQL versions.
diff "$DUMPLING_BASE_NAME/expect/naughty_strings.t.sql" "$DUMPLING_OUTPUT_DIR/naughty_strings.t.000000000.sql"

# run with compress option
rm "$DUMPLING_OUTPUT_DIR/naughty_strings.t.000000000.sql"
run_dumpling --escape-backslash=false --compress "gzip"
file_should_exist "$DUMPLING_OUTPUT_DIR/naughty_strings.t.000000000.sql.gz"
gzip "$DUMPLING_OUTPUT_DIR/naughty_strings.t.000000000.sql.gz" -d
diff "$DUMPLING_BASE_NAME/expect/naughty_strings.t.sql" "$DUMPLING_OUTPUT_DIR/naughty_strings.t.000000000.sql"

run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.escape-schema.sql"
run_sql_file "$DUMPLING_BASE_NAME/data/naughty_strings.escape.sql"
run_dumpling --escape-backslash=true
# FIXME should compare the schemas too, but they differ too much among MySQL versions.
diff "$DUMPLING_BASE_NAME/expect/naughty_strings.escape.sql" "$DUMPLING_OUTPUT_DIR/naughty_strings.escape.000000000.sql"
