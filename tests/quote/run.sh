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

mkdir -p "$DUMPLING_OUTPUT_DIR"/data
cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table.000000000.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable.000000000.sql"
cp "$DUMPLING_BASE_NAME/data/quote-database.quote-table-schema.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable-schema.sql"
cp "$DUMPLING_BASE_NAME/data/quote-database-schema-create.sql" "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase-schema-create.sql"

db="quo\`te/database"
run_sql "drop database if exists \`quo\`\`te/database\`"
run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase-schema-create.sql"
export DUMPLING_TEST_DATABASE=$db

run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable-schema.sql"
run_sql_file "$DUMPLING_OUTPUT_DIR/data/quo\`te%2Fdatabase.quo\`te%2Ftable.000000000.sql"

run_dumpling

for file_path in "$DUMPLING_OUTPUT_DIR"/data/*; do
  base_name=$(basename "$file_path")
  file_should_exist "$DUMPLING_OUTPUT_DIR/data/$base_name"
  file_should_exist "$DUMPLING_OUTPUT_DIR/$base_name"
  diff "$DUMPLING_OUTPUT_DIR/data/$base_name" "$DUMPLING_OUTPUT_DIR/$base_name"
done
