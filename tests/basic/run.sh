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
cur=$(cd `dirname $0`; pwd)

DB_NAME="basic"
TABLE_NAME="t"

# Test for simple case.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\`;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int);"
run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` values (1), (2);"

run_dumpling

cnt=`grep -w "(.*)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql|wc -l`
echo "records count is ${cnt}"
[ "$cnt" = 2 ]

# Test for simple WHERE case.
run_sql "drop database if exists \`$DB_NAME\`;"
run_sql "create database \`$DB_NAME\`;"
run_sql "create table \`$DB_NAME\`.\`$TABLE_NAME\` (a int);"

seq 10 | xargs -I_ run_sql "insert into \`$DB_NAME\`.\`$TABLE_NAME\` values (_);"

run_dumpling --where "a >= 3 and a <= 9" -f "$DB_NAME.$TABLE_NAME"

actual=$(grep -w "(.*)" ${DUMPLING_OUTPUT_DIR}/${DB_NAME}.${TABLE_NAME}.000000000.sql | cut -c2-2)
expected=$(seq 3 9)
[ "$actual" = "$expected" ]
