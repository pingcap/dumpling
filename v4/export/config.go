// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

const (
	flagDatabase                 = "database"
	flagTablesList               = "tables-list"
	flagHost                     = "host"
	flagUser                     = "user"
	flagPort                     = "port"
	flagPassword                 = "password"
	flagAllowCleartextPasswords  = "allow-cleartext-passwords"
	flagThreads                  = "threads"
	flagFilesize                 = "filesize"
	flagStatementSize            = "statement-size"
	flagOutput                   = "output"
	flagLoglevel                 = "loglevel"
	flagLogfile                  = "logfile"
	flagLogfmt                   = "logfmt"
	flagConsistency              = "consistency"
	flagSnapshot                 = "snapshot"
	flagNoViews                  = "no-views"
	flagStatusAddr               = "status-addr"
	flagRows                     = "rows"
	flagWhere                    = "where"
	flagEscapeBackslash          = "escape-backslash"
	flagFiletype                 = "filetype"
	flagNoHeader                 = "no-header"
	flagNoSchemas                = "no-schemas"
	flagNoData                   = "no-data"
	flagCsvNullValue             = "csv-null-value"
	flagSql                      = "sql"
	flagFilter                   = "filter"
	flagCaseSensitive            = "case-sensitive"
	flagDumpEmptyDatabase        = "dump-empty-database"
	flagTidbMemQuotaQuery        = "tidb-mem-quota-query"
	flagCA                       = "ca"
	flagCert                     = "cert"
	flagKey                      = "key"
	flagCsvSeparator             = "csv-separator"
	flagCsvDelimiter             = "csv-delimiter"
	flagOutputFilenameTemplate   = "output-filename-template"
	flagCompleteInsert           = "complete-insert"
	flagParams                   = "params"
	flagReadTimeout              = "read-timeout"
	flagTransactionalConsistency = "transactional-consistency"
	flagCompress                 = "compress"

	FlagHelp = "help"
)

type Config struct {
	storage.BackendOptions

	Databases               []string
	Host                    string
	User                    string
	Port                    int
	Password                string `json:"-"`
	AllowCleartextPasswords bool
	Security                struct {
		CAPath   string
		CertPath string
		KeyPath  string
	}

	Threads int

	LogLevel  string
	LogFile   string
	LogFormat string
	Logger    *zap.Logger `json:"-"`

	FileSize      uint64
	StatementSize uint64
	OutputDirPath string
	ServerInfo    ServerInfo
	SortByPk      bool
	Tables        DatabaseTables
	StatusAddr    string
	Snapshot      string
	Consistency   string
	NoViews       bool
	NoHeader      bool
	NoSchemas     bool
	NoData        bool
	CsvNullValue  string
	Sql           string
	CsvSeparator  string
	CsvDelimiter  string
	ReadTimeout   time.Duration
	CompressType  storage.CompressType

	TableFilter              filter.Filter `json:"-"`
	Rows                     uint64
	Where                    string
	FileType                 string
	CompleteInsert           bool
	TransactionalConsistency bool
	EscapeBackslash          bool
	DumpEmptyDatabase        bool
	OutputFileTemplate       *template.Template `json:"-"`
	TiDBMemQuotaQuery        uint64
	SessionParams            map[string]interface{}

	PosAfterConnect bool
	Labels          prometheus.Labels `json:"-"`
}

func DefaultConfig() *Config {
	allFilter, _ := filter.Parse([]string{"*.*"})
	return &Config{
		Databases:          nil,
		Host:               "127.0.0.1",
		User:               "root",
		Port:               3306,
		Password:           "",
		Threads:            4,
		Logger:             nil,
		StatusAddr:         ":8281",
		FileSize:           UnspecifiedSize,
		StatementSize:      DefaultStatementSize,
		OutputDirPath:      ".",
		ServerInfo:         ServerInfoUnknown,
		SortByPk:           true,
		Tables:             nil,
		Snapshot:           "",
		Consistency:        consistencyTypeAuto,
		NoViews:            true,
		Rows:               UnspecifiedSize,
		Where:              "",
		FileType:           "sql",
		NoHeader:           false,
		NoSchemas:          false,
		NoData:             false,
		CsvNullValue:       "\\N",
		Sql:                "",
		TableFilter:        allFilter,
		DumpEmptyDatabase:  true,
		SessionParams:      make(map[string]interface{}),
		OutputFileTemplate: DefaultOutputFileTemplate,
		PosAfterConnect:    false,
	}
}

func (config *Config) String() string {
	cfg, err := json.Marshal(config)
	if err != nil {
		log.Error("marshal config to json", zap.Error(err))
	}
	return string(cfg)
}

// GetDSN generates DSN from Config
func (conf *Config) GetDSN(db string) string {
	// maxAllowedPacket=0 can be used to automatically fetch the max_allowed_packet variable from server on every connection.
	// https://github.com/go-sql-driver/mysql#maxallowedpacket
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&readTimeout=%s&writeTimeout=30s&interpolateParams=true&maxAllowedPacket=0",
		conf.User, conf.Password, conf.Host, conf.Port, db, conf.ReadTimeout)
	if len(conf.Security.CAPath) > 0 {
		dsn += "&tls=dumpling-tls-target"
	}
	if conf.AllowCleartextPasswords {
		dsn += "&allowCleartextPasswords=1"
	}
	return dsn
}

func timestampDirName() string {
	return fmt.Sprintf("./export-%s", time.Now().Format(time.RFC3339))
}

func (conf *Config) DefineFlags(flags *pflag.FlagSet) {
	storage.DefineFlags(flags)
	flags.StringSliceP(flagDatabase, "B", nil, "Databases to dump")
	flags.StringSliceP(flagTablesList, "T", nil, "Comma delimited table list to dump; must be qualified table names")
	flags.StringP(flagHost, "h", "127.0.0.1", "The host to connect to")
	flags.StringP(flagUser, "u", "root", "Username with privileges to run the dump")
	flags.IntP(flagPort, "P", 4000, "TCP/IP port to connect to")
	flags.StringP(flagPassword, "p", "", "User password")
	flags.Bool(flagAllowCleartextPasswords, false, "Allow passwords to be sent in cleartext (warning: don't use without TLS)")
	flags.IntP(flagThreads, "t", 4, "Number of goroutines to use, default 4")
	flags.StringP(flagFilesize, "F", "", "The approximate size of output file")
	flags.Uint64P(flagStatementSize, "s", DefaultStatementSize, "Attempted size of INSERT statement in bytes")
	flags.StringP(flagOutput, "o", timestampDirName(), "Output directory")
	flags.String(flagLoglevel, "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")
	flags.StringP(flagLogfile, "L", "", "Log file `path`, leave empty to write to console")
	flags.String(flagLogfmt, "text", "Log `format`: {text|json}")
	flags.String(flagConsistency, consistencyTypeAuto, "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	flags.String(flagSnapshot, "", "Snapshot position (uint64 from pd timestamp for TiDB). Valid only when consistency=snapshot")
	flags.BoolP(flagNoViews, "W", true, "Do not dump views")
	flags.String(flagStatusAddr, ":8281", "dumpling API server and pprof addr")
	flags.Uint64P(flagRows, "r", UnspecifiedSize, "Split table into chunks of this many rows, default unlimited")
	flags.String(flagWhere, "", "Dump only selected records")
	flags.Bool(flagEscapeBackslash, true, "use backslash to escape special characters")
	flags.String(flagFiletype, "sql", "The type of export file (sql/csv)")
	flags.Bool(flagNoHeader, false, "whether not to dump CSV table header")
	flags.BoolP(flagNoSchemas, "m", false, "Do not dump table schemas with the data")
	flags.BoolP(flagNoData, "d", false, "Do not dump table data")
	flags.String(flagCsvNullValue, "\\N", "The null value used when export to csv")
	flags.StringP(flagSql, "S", "", "Dump data with given sql. This argument doesn't support concurrent dump")
	flags.StringSliceP(flagFilter, "f", []string{"*.*", DefaultTableFilter}, "filter to select which tables to dump")
	flags.Bool(flagCaseSensitive, false, "whether the filter should be case-sensitive")
	flags.Bool(flagDumpEmptyDatabase, true, "whether to dump empty database")
	flags.Uint64(flagTidbMemQuotaQuery, DefaultTiDBMemQuotaQuery, "The maximum memory limit for a single SQL statement, in bytes. Default: 32GB")
	flags.String(flagCA, "", "The path name to the certificate authority file for TLS connection")
	flags.String(flagCert, "", "The path name to the client certificate file for TLS connection")
	flags.String(flagKey, "", "The path name to the client private key file for TLS connection")
	flags.String(flagCsvSeparator, ",", "The separator for csv files, default ','")
	flags.String(flagCsvDelimiter, "\"", "The delimiter for values in csv files, default '\"'")
	flags.String(flagOutputFilenameTemplate, "", "The output filename template (without file extension)")
	flags.Bool(flagCompleteInsert, false, "Use complete INSERT statements that include column names")
	flags.StringToString(flagParams, nil, `Extra session variables used while dumping, accepted format: --params "character_set_client=latin1,character_set_connection=latin1"`)
	flags.Bool(FlagHelp, false, "Print help message and quit")
	flags.Duration(flagReadTimeout, 15*time.Minute, "I/O read timeout for db connection.")
	flags.MarkHidden(flagReadTimeout)
	flags.Bool(flagTransactionalConsistency, true, "Only support transactional consistency")
	flags.StringP(flagCompress, "c", "", "Compress output file type, support 'gzip', 'no-compression' now")
}

// GetDSN generates DSN from Config
func (conf *Config) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	conf.Databases, err = flags.GetStringSlice(flagDatabase)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Host, err = flags.GetString(flagHost)
	if err != nil {
		return errors.Trace(err)
	}
	conf.User, err = flags.GetString(flagUser)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Port, err = flags.GetInt(flagPort)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Password, err = flags.GetString(flagPassword)
	if err != nil {
		return errors.Trace(err)
	}
	conf.AllowCleartextPasswords, err = flags.GetBool(flagAllowCleartextPasswords)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Threads, err = flags.GetInt(flagThreads)
	if err != nil {
		return errors.Trace(err)
	}
	conf.StatementSize, err = flags.GetUint64(flagStatementSize)
	if err != nil {
		return errors.Trace(err)
	}
	conf.OutputDirPath, err = flags.GetString(flagOutput)
	if err != nil {
		return errors.Trace(err)
	}
	conf.LogLevel, err = flags.GetString(flagLoglevel)
	if err != nil {
		return errors.Trace(err)
	}
	conf.LogFile, err = flags.GetString(flagLogfile)
	if err != nil {
		return errors.Trace(err)
	}
	conf.LogFormat, err = flags.GetString(flagLogfmt)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Consistency, err = flags.GetString(flagConsistency)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Snapshot, err = flags.GetString(flagSnapshot)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoViews, err = flags.GetBool(flagNoViews)
	if err != nil {
		return errors.Trace(err)
	}
	conf.StatusAddr, err = flags.GetString(flagStatusAddr)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Rows, err = flags.GetUint64(flagRows)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Where, err = flags.GetString(flagWhere)
	if err != nil {
		return errors.Trace(err)
	}
	conf.EscapeBackslash, err = flags.GetBool(flagEscapeBackslash)
	if err != nil {
		return errors.Trace(err)
	}
	conf.FileType, err = flags.GetString(flagFiletype)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoHeader, err = flags.GetBool(flagNoHeader)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoSchemas, err = flags.GetBool(flagNoSchemas)
	if err != nil {
		return errors.Trace(err)
	}
	conf.NoData, err = flags.GetBool(flagNoData)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CsvNullValue, err = flags.GetString(flagCsvNullValue)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Sql, err = flags.GetString(flagSql)
	if err != nil {
		return errors.Trace(err)
	}
	conf.DumpEmptyDatabase, err = flags.GetBool(flagDumpEmptyDatabase)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Security.CAPath, err = flags.GetString(flagCA)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Security.CertPath, err = flags.GetString(flagCert)
	if err != nil {
		return errors.Trace(err)
	}
	conf.Security.KeyPath, err = flags.GetString(flagKey)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CsvSeparator, err = flags.GetString(flagCsvSeparator)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CsvDelimiter, err = flags.GetString(flagCsvDelimiter)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CompleteInsert, err = flags.GetBool(flagCompleteInsert)
	if err != nil {
		return errors.Trace(err)
	}
	conf.ReadTimeout, err = flags.GetDuration(flagReadTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	conf.TransactionalConsistency, err = flags.GetBool(flagTransactionalConsistency)
	if err != nil {
		return errors.Trace(err)
	}
	conf.TiDBMemQuotaQuery, err = flags.GetUint64(flagTidbMemQuotaQuery)
	if err != nil {
		return errors.Trace(err)
	}

	if conf.Threads <= 0 {
		return errors.Errorf("--threads is set to %d. It should be greater than 0", conf.Threads)
	}

	if conf.SessionParams == nil {
		conf.SessionParams = make(map[string]interface{})
	}

	tablesList, err := flags.GetStringSlice(flagTablesList)
	if err != nil {
		return errors.Trace(err)
	}
	fileSizeStr, err := flags.GetString(flagFilesize)
	if err != nil {
		return errors.Trace(err)
	}
	filters, err := flags.GetStringSlice(flagFilter)
	if err != nil {
		return errors.Trace(err)
	}
	caseSensitive, err := flags.GetBool(flagCaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}
	outputFilenameFormat, err := flags.GetString(flagOutputFilenameTemplate)
	if err != nil {
		return errors.Trace(err)
	}
	params, err := flags.GetStringToString(flagParams)
	if err != nil {
		return errors.Trace(err)
	}

	conf.TableFilter, err = ParseTableFilter(tablesList, filters)
	if err != nil {
		return errors.Errorf("failed to parse filter: %s", err)
	}

	if !caseSensitive {
		conf.TableFilter = filter.CaseInsensitive(conf.TableFilter)
	}

	conf.FileSize, err = ParseFileSize(fileSizeStr)
	if err != nil {
		return errors.Trace(err)
	}

	if outputFilenameFormat == "" && conf.Sql != "" {
		outputFilenameFormat = DefaultAnonymousOutputFileTemplateText
	}
	tmpl, err := ParseOutputFileTemplate(outputFilenameFormat)
	if err != nil {
		return errors.Errorf("failed to parse output filename template (--output-filename-template '%s')\n", outputFilenameFormat)
	}
	conf.OutputFileTemplate = tmpl

	compressType, err := flags.GetString(flagCompress)
	if err != nil {
		return errors.Trace(err)
	}
	conf.CompressType, err = ParseCompressType(compressType)
	if err != nil {
		return errors.Trace(err)
	}

	for k, v := range params {
		conf.SessionParams[k] = v
	}

	err = conf.BackendOptions.ParseFromFlags(pflag.CommandLine)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func ParseFileSize(fileSizeStr string) (uint64, error) {
	if len(fileSizeStr) == 0 {
		return UnspecifiedSize, nil
	} else if fileSizeMB, err := strconv.ParseUint(fileSizeStr, 10, 64); err == nil {
		fmt.Printf("Warning: -F without unit is not recommended, try using `-F '%dMiB'` in the future\n", fileSizeMB)
		return fileSizeMB * units.MiB, nil
	} else if size, err := units.RAMInBytes(fileSizeStr); err == nil {
		return uint64(size), nil
	}
	return 0, errors.Errorf("failed to parse filesize (-F '%s')", fileSizeStr)
}

func ParseTableFilter(tablesList, filters []string) (filter.Filter, error) {
	if len(tablesList) == 0 {
		return filter.Parse(filters)
	}

	// only parse -T when -f is default value. otherwise bail out.
	if !sameStringArray(filters, []string{"*.*", DefaultTableFilter}) {
		return nil, errors.New("cannot pass --tables-list and --filter together")
	}

	tableNames := make([]filter.Table, 0, len(tablesList))
	for _, table := range tablesList {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) < 2 {
			return nil, errors.Errorf("--tables-list only accepts qualified table names, but `%s` lacks a dot", table)
		}
		tableNames = append(tableNames, filter.Table{Schema: parts[0], Name: parts[1]})
	}

	return filter.NewTablesFilter(tableNames...), nil
}

func ParseCompressType(compressType string) (storage.CompressType, error) {
	switch compressType {
	case "", "no-compression":
		return storage.NoCompression, nil
	case "gzip", "gz":
		return storage.Gzip, nil
	default:
		return storage.NoCompression, errors.Errorf("unknown compress type %s", compressType)
	}
}

func (config *Config) createExternalStorage(ctx context.Context) (storage.ExternalStorage, error) {
	b, err := storage.ParseBackend(config.OutputDirPath, &config.BackendOptions)
	if err != nil {
		return nil, err
	}
	httpClient := http.DefaultClient
	httpClient.Timeout = 30 * time.Second
	maxIdleConnsPerHost := http.DefaultMaxIdleConnsPerHost
	if config.Threads > maxIdleConnsPerHost {
		maxIdleConnsPerHost = config.Threads
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
	httpClient.Transport = transport

	return storage.New(ctx, b, &storage.ExternalStorageOptions{
		HTTPClient: httpClient,
	})
}

const (
	UnspecifiedSize          = 0
	DefaultTiDBMemQuotaQuery = 32 << 30
	DefaultStatementSize     = 1000000
	TiDBMemQuotaQueryName    = "tidb_mem_quota_query"
	DefaultTableFilter       = "!/^(mysql|sys|INFORMATION_SCHEMA|PERFORMANCE_SCHEMA|METRICS_SCHEMA|INSPECTION_SCHEMA)$/.*"

	defaultDumpThreads         = 128
	defaultDumpGCSafePointTTL  = 5 * 60
	dumplingServiceSafePointID = "dumpling"
	defaultEtcdDialTimeOut     = 3 * time.Second
)

var (
	gcSafePointVersion = semver.New("4.0.0")
	tableSampleVersion = semver.New("5.0.0")
)

type ServerInfo struct {
	ServerType    ServerType
	ServerVersion *semver.Version
}

var ServerInfoUnknown = ServerInfo{
	ServerType:    ServerTypeUnknown,
	ServerVersion: nil,
}

var versionRegex = regexp.MustCompile(`^\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
var tidbVersionRegex = regexp.MustCompile(`-[v]?\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)

func ParseServerInfo(src string) ServerInfo {
	log.Debug("parse server info", zap.String("server info string", src))
	lowerCase := strings.ToLower(src)
	serverInfo := ServerInfo{}
	if strings.Contains(lowerCase, "tidb") {
		serverInfo.ServerType = ServerTypeTiDB
	} else if strings.Contains(lowerCase, "mariadb") {
		serverInfo.ServerType = ServerTypeMariaDB
	} else if versionRegex.MatchString(lowerCase) {
		serverInfo.ServerType = ServerTypeMySQL
	} else {
		serverInfo.ServerType = ServerTypeUnknown
	}

	log.Info("detect server type",
		zap.String("type", serverInfo.ServerType.String()))

	var versionStr string
	if serverInfo.ServerType == ServerTypeTiDB {
		versionStr = tidbVersionRegex.FindString(src)[1:]
		versionStr = strings.TrimPrefix(versionStr, "v")
	} else {
		versionStr = versionRegex.FindString(src)
	}

	var err error
	serverInfo.ServerVersion, err = semver.NewVersion(versionStr)
	if err != nil {
		log.Warn("fail to parse version",
			zap.String("version", versionStr))
		return serverInfo
	}

	log.Info("detect server version",
		zap.String("version", serverInfo.ServerVersion.String()))
	return serverInfo
}

type ServerType int8

func (s ServerType) String() string {
	if s >= ServerTypeAll {
		return ""
	}
	return serverTypeString[s]
}

const (
	ServerTypeUnknown = iota
	ServerTypeMySQL
	ServerTypeMariaDB
	ServerTypeTiDB

	ServerTypeAll
)

var serverTypeString []string

func init() {
	serverTypeString = make([]string, ServerTypeAll)
	serverTypeString[ServerTypeUnknown] = "Unknown"
	serverTypeString[ServerTypeMySQL] = "MySQL"
	serverTypeString[ServerTypeMariaDB] = "MariaDB"
	serverTypeString[ServerTypeTiDB] = "TiDB"
}

func adjustConfig(conf *Config, fns ...func(*Config) error) error {
	for _, f := range fns {
		err := f(conf)
		if err != nil {
			return err
		}
	}
	return nil
}

func initLogger(conf *Config) error {
	if conf.Logger != nil {
		log.SetAppLogger(conf.Logger)
	} else {
		err := log.InitAppLogger(&log.Config{
			Level:  conf.LogLevel,
			File:   conf.LogFile,
			Format: conf.LogFormat,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func registerTLSConfig(conf *Config) error {
	if len(conf.Security.CAPath) > 0 {
		tlsConfig, err := utils.ToTLSConfig(conf.Security.CAPath, conf.Security.CertPath, conf.Security.KeyPath)
		if err != nil {
			return err
		}
		err = mysql.RegisterTLSConfig("dumpling-tls-target", tlsConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateSpecifiedSQL(conf *Config) error {
	if conf.Sql != "" && conf.Where != "" {
		return errors.New("can't specify both --sql and --where at the same time. Please try to combine them into --sql")
	}
	return nil
}

func validateFileFormat(conf *Config) error {
	conf.FileType = strings.ToLower(conf.FileType)
	switch conf.FileType {
	case "sql", "csv":
		return nil
	}
	return errors.Errorf("unknown config.FileType '%s'", conf.FileType)
}
