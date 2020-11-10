module github.com/pingcap/dumpling

go 1.13

require (
	github.com/DATA-DOG/go-sqlmock v1.4.1
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/go-delve/delve v1.5.0 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/google/go-dap v0.3.0 // indirect
	github.com/lichunzhu/go-mysql v1.1.0
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/peterh/liner v1.2.0 // indirect
	github.com/pingcap/br v0.0.0-20200925095602-bf9cc603382e
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20200902104258-eba4f1d8f6de
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20200910095337-6b893f12be43
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pingcap/tidb-tools v4.0.5-0.20200820092506-34ea90c93237+incompatible
	github.com/pkg/errors v0.9.1
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/siddontang/go-mysql v1.1.0
	github.com/sirupsen/logrus v1.7.0 // indirect
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.1.1 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/tikv/pd v1.1.0-beta.0.20200825070655-6b09f3acbb1f
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.starlark.net v0.0.0-20201014215153-dff0ae5b4820 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/arch v0.0.0-20201008161808-52c3e6f60cff // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20201107080550-4d91cf3a1aaf // indirect
	golang.org/x/tools v0.0.0-20200823205832-c024452afbcd // indirect
)

replace github.com/lichunzhu/go-mysql => ./vendor/go-mysql
