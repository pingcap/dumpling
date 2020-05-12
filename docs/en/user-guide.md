# Dumpling manual

**Dumpling** is a tool and a Go library for creating SQL dump (CSV/SQL format) from a MySQL-compatible database.

It is intended to replace `mysqldump` and `mydumper` when targeting TiDB, as a result, its basic usage is similar to Mydumper. 

The following table lists main parameters of dumpling.


| Parameter |     |
| --------| --- |
| -B or --database | Dump the specified database. |
| -H or --host | Host to connect to. (default 127.0.0.1) |
| -t or --threads | Number of threads for concurrent backup. |
| -r or --rows | Split table into chunks of this many rows. (default unlimited) |
| --loglevel | Log level. (debug, info, warn, error, dpanic, panic, fatal, default "info") |
| -d or --no-data | Don't dump data, for schema-only case. |
| --no-header | DUmp table csv without header. |
| -W or --no-views | Don't dump views. (default true) |
| -m or --no-schemas | Don't dump schemas, dump data only. |
| -s or --statement-size | Control the size of Insert Statement, unit is byte. |
| -F or --filesize | The approximate size of output file, unit is byte. |
| --filetype| The type of dump file. (sql/csv, default "sql")           |
| -o or --output | Output directory. (We have a default value based on time.) |
| --consistency | Which consistency control to use (default `auto`):<br>`flush`: Use FTWRL (flush tables with read lock)<br>`snapshot`: use a snapshot at given timestamp<br>`lock`: execute lock tables read for all tables need to be locked <br>`none`: dump without locking, cannot guarantee consistency <br>`auto`: `flush` on MySQL, `snapshot` on TiDB |
| --snapshot | Snapshot position. Valid only when consistency=snapshot. |
| --where | Specify the dump range by where condition. Dump only selected records. |
| -p or --password | User password. |
| -P or --port | TCP/IP port to connect to. (default 4000) |
| -u or --user | Username with privileges to run the dump. (default "root") |

More detailed usage can be find by the flag `-h` or `--help`.

## Mydumper Reference

[Mydumper usage](https://github.com/maxbube/mydumper/blob/master/docs/mydumper_usage.rst)

[TiDB Mydumper Manual](https://pingcap.com/docs/stable/reference/tools/mydumper/)

## Download

[nightly](https://download.pingcap.org/dumpling-nightly-linux-amd64.tar.gz)
