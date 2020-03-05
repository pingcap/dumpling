#!/bin/sh

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="e2e"
TABLE_NAME="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists $DB_NAME;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists $DB_NAME;"

# build data on mysql
run_sql "create database $DB_NAME;"
run_sql "create table $DB_NAME.$TABLE_NAME (a int(255));"

# insert 100 records
i=0; while [ $i -lt 100 ]; do
  run_sql "insert into $DB_NAME.$TABLE_NAME values (\"$i\");"
  i=$(( i + 1 ))
done

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling

# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml


