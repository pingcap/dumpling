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

DB_NAME="empty_test"

# drop database on mysql
run_sql "drop database if exists \`$DB_NAME\`;"

# build data on mysql
run_sql "create database \`$DB_NAME\`;"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling

sql="CREATE DATABASE \`$DB_NAME\`"
cnt=$(sed "s/$sql/$sql\n/g" $DUMPLING_OUTPUT_DIR/$DB_NAME-schema-create.sql | grep -c "$sql") || true
[ $cnt = 1 ]

