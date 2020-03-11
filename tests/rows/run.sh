#!/bin/sh

set -eu
cur=$(cd `dirname $0`; pwd)

DB_NAME="rows"
TABLE_NAME="t"

# drop database on tidb
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists $DB_NAME;"

# drop database on mysql
export DUMPLING_TEST_PORT=3306
run_sql "drop database if exists $DB_NAME;"

# build data on mysql
run_sql "create database $DB_NAME;"
run_sql "create table $DB_NAME.$TABLE_NAME (id int not null auto_increment primary key, a int(255));"

# insert 2000 records
run_sql "insert into $DB_NAME.$TABLE_NAME (a) values $(seq -s, 2000 | sed 's/,*$//g' | sed "s/[0-9]*/('1')/g");"

# dumping
export DUMPLING_TEST_DATABASE=$DB_NAME
run_dumpling --rows 100 --loglevel debug

# the dumping result is expected to be:
# 10 files for insertion
file_num=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -iname "$DB_NAME.$TABLE_NAME.*.sql" | wc -l)
if [ "$file_num" -ne 20 ]; then
  echo "obtain file number: $file_num, but expect: 20" && exit 1
fi

cat "$cur/conf/lightning.toml"
# use lightning import data to tidb
run_lightning $cur/conf/lightning.toml

# check mysql and tidb data
check_sync_diff $cur/conf/diff_config.toml


