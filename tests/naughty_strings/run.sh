#!/bin/sh

set -eu

run_sql "DROP DATABASE IF EXISTS naughty_strings"
run_sql_file "$BASE_NAME/data/naughty_strings-schema-create.sql"
export TEST_DATABASE="naughty_strings"
run_sql_file "$BASE_NAME/data/naughty_strings.t-schema.sql"
run_sql_file "$BASE_NAME/data/naughty_strings.t.sql"
run_dumpling "naughty_strings"
diff --color "$BASE_NAME/data" "$OUTPUT_DIR"
