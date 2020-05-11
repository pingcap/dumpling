# Dumpling 使用手册

[Dumpling](https://github.com/pingcap/dumpling) 是支持以 SQL 文本或者 CSV 格式将 MySQL/TiDB 数据库的数据的导出工具。

设计初衷是为了替代 [Mydumper](https://github.com/pingcap/mydumper), 所以基本用法可以参考 Mydumper, 
当然在实现中没有完全照搬 Mydumper, 因此存在与 Mydumper 不同的用法。

下表罗列了一些主要参数以及与 Mydumper 不同之处。

| 主要参数 |     |
| --------| --- |
| -H 或 --host| 链接节点地址(默认 "127.0.0.1")|
| -t 或 --threads | 备份并发线程数|
| -r 或 --rows |将 table 划分成 row 行数据，一般针对大表操作并发生成多个文件。|
| -s 或--statement-size | 控制 Insert Statement 的大小，单位 bytes |
| -F 或 --filesize | 将 table 数据划分出来的文件大小, 单位 bytes |
| -o, --outputdir | 设置导出文件路径 |
| --consistency | flush: dump 前用 FTWRL <br> snapshot: 通过 tso 指定 dump 位置 <br> lock: 对需要 dump 的所有表执行 lock tables read <br> none: 不加锁 dump，无法保证一致性 <br> auto: MySQL flush, TiDB snapshot|
| --where | 对备份的数据表通过 where 条件指定范围 |

其它基本参数 -u -p -P -B 用法跟 Mydumper 保持一致。

更多具体用法可以使用 -h, --help 进行查看。

## Mydumper 相关参考

[Mydumper usage](https://github.com/maxbube/mydumper/blob/master/docs/mydumper_usage.rst)

[TiDB Mydumper 使用文档](https://pingcap.com/docs-cn/stable/reference/tools/mydumper/)

## Dumpling 下载链接

[nightly](https://download.pingcap.org/dumpling-nightly-linux-amd64.tar.gz)