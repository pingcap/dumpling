# Dumpling manual

[Dumpling](https://github.com/pingcap/dumpling) is a tool that supports export MySQL/TiDB data as SQL text or CSV file.

It's designed to replace [Mydumper](https://github.com/pingcap/mydumper), so you can learn its basic usage from Mydumper. Of course, we didn't make a replica of Mydumper, so there are difference between them.

The following table lists main parameters of dumpling.


| Parameter |     |
| --------| --- |
| -B or --database | Export to specifcated database. |
| -H or --host | Address of node to connect. (default 127.0.0.1) |
| -t or --threads | Thread number for concurrence backup. |
| -r or --rows | Split table to rows of data. When working with big tables, normally we generate many files concurrencly. |
| --loglevel | Log level. (debug, info, warn, error, dpanic, panic, fatal, default "info") |
| -d or --no-data | Don't export data, for schema-only case. |
| --no-header | Export table csv without header. |
| -W or --no-views | Don't export views. (default true) |
| -m or --no-schemas | Don't export schema, export data only. |
| -s or --statement-size | Control the size of Insert Statement, unit is byte. |
| -F or --filesize | The approximate size of output file, unit is byte. |
| --filetype| The type of export file. (sql/csv, default "sql")           |
| -o or --output | Output directory. (We have a default value based on time.) |
| --consistency | Consistency level during dumping: (deault `auto`)<br>`flush`: FTWRL before dump <br>`snapshot`: specify dump timestamp by tso <br>`lock`: execute lock tables read for all tables need to be locked <br>`none`: dump without locking, cannot guarantee consistency <br>`auto`: flush on MySQL, snapshot on TiDB |
| --snapshot | Snapshot tso, only effective when consistency=snapshot. |
| --where | Specify the dump range by where condition. |
| -p or --password | User password. |
| -P or --port | TCP/IP port to connect to. (default 4000) |
| -u or --user | Username with privileges to run the dump. (default "root") |

More detailed usage can be find by the flag `-h` or `--help`.

## Mydumper Reference

[Mydumper usage](https://github.com/maxbube/mydumper/blob/master/docs/mydumper_usage.rst)

[TiDB Mydumper Manual (Chinese)](https://pingcap.com/docs-cn/stable/reference/tools/mydumper/)

## Dumpling Downloading

[nightly](https://download.pingcap.org/dumpling-nightly-linux-amd64.tar.gz)