module github.com/pingcap/dumpling

go 1.16

require (
	cloud.google.com/go/storage v1.16.1 // indirect
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/coreos/go-semver v0.3.0
	github.com/docker/go-units v0.4.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/pingcap/badger v1.5.1-0.20210831093107-2f6cb8008145 // indirect
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/parser v0.0.0-20210831085004-b5390aa83f65
	github.com/pingcap/tidb v1.1.0-beta.0.20211013051927-1ff46a93327d
	github.com/pingcap/tidb-tools v5.2.0-alpha.0.20210727084616-915b22e4d42c+incompatible
	github.com/pingcap/tipb v0.0.0-20210802080519-94b831c6db55 // indirect
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20210913094925-a8fa8acd44e7 // indirect
	github.com/tikv/pd v1.1.0-beta.0.20210818112400-0c5667766690
	github.com/xitongsys/parquet-go v1.6.0 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/goleak v1.1.10
	go.uber.org/zap v1.19.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
