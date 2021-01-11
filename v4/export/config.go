// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
	flagSQL                      = "sql"
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

	// FlagHelp represents the help flag
	FlagHelp = "help"
	// FlagVersion represents the version flag
	FlagVersion = "version"
)

// Config is the dump config for dumpling
type Config struct {
	FlagSet *pflag.FlagSet
	storage.BackendOptions

	AllowCleartextPasswords  bool
	SortByPk                 bool
	NoViews                  bool
	NoHeader                 bool
	NoSchemas                bool
	NoData                   bool
	CompleteInsert           bool
	TransactionalConsistency bool
	EscapeBackslash          bool
	DumpEmptyDatabase        bool
	PosAfterConnect          bool
	CompressType             storage.CompressType

	Host     string
	Port     int
	Threads  int
	User     string
	Password string `json:"-"`
	Security struct {
		CAPath   string
		CertPath string
		KeyPath  string
	}

	LogLevel      string
	LogFile       string
	LogFormat     string
	OutputDirPath string
	StatusAddr    string
	Snapshot      string
	Consistency   string
	CsvNullValue  string
	SQL           string
	CsvSeparator  string
	CsvDelimiter  string
	Databases     []string

	TableFilter        filter.Filter `json:"-"`
	Where              string
	FileType           string
	ServerInfo         ServerInfo
	Logger             *zap.Logger        `json:"-"`
	OutputFileTemplate *template.Template `json:"-"`
	Rows               uint64
	ReadTimeout        time.Duration
	TiDBMemQuotaQuery  uint64
	FileSize           uint64
	StatementSize      uint64
	SessionParams      map[string]interface{}
	Labels             prometheus.Labels `json:"-"`
	Tables             DatabaseTables
}

// DefaultConfig returns the default export Config for dumpling
func DefaultConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
	cfg.FlagSet.Usage = func() {
		fmt.Fprint(os.Stderr, "Dumpling is a CLI tool that helps you dump MySQL/TiDB data\n\nUsage:\n  dumpling [flags]\n\nFlags:\n")
		cfg.FlagSet.PrintDefaults()
	}

	flags := cfg.FlagSet
	flags.StringSliceVarP(&cfg.Databases, flagDatabase, "B", nil, "Databases to dump")
	flags.StringVarP(&cfg.Host, flagHost, "h", "127.0.0.1", "The host to connect to")
	flags.StringVarP(&cfg.User, flagUser, "u", "root", "Username with privileges to run the dump")
	flags.IntVarP(&cfg.Port, flagPort, "P", 4000, "TCP/IP port to connect to")
	flags.StringVarP(&cfg.Password, flagPassword, "p", "", "User password")
	flags.BoolVar(&cfg.AllowCleartextPasswords, flagAllowCleartextPasswords, false, "Allow passwords to be sent in cleartext (warning: don't use without TLS)")
	flags.IntVarP(&cfg.Threads, flagThreads, "t", 4, "Number of goroutines to use, default 4")
	flags.Uint64VarP(&cfg.StatementSize, flagStatementSize, "s", DefaultStatementSize, "Attempted size of INSERT statement in bytes")
	flags.StringVarP(&cfg.OutputDirPath, flagOutput, "o", timestampDirName(), "Output directory")
	flags.StringVar(&cfg.LogLevel, flagLoglevel, "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")
	flags.StringVarP(&cfg.LogFile, flagLogfile, "L", "", "Log file `path`, leave empty to write to console")
	flags.StringVar(&cfg.LogFormat, flagLogfmt, "text", "Log `format`: {text|json}")
	flags.StringVar(&cfg.Consistency, flagConsistency, consistencyTypeAuto, "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	flags.StringVar(&cfg.Snapshot, flagSnapshot, "", "Snapshot position (uint64 from pd timestamp for TiDB). Valid only when consistency=snapshot")
	flags.BoolVarP(&cfg.NoViews, flagNoViews, "W", true, "Do not dump views")
	flags.StringVar(&cfg.StatusAddr, flagStatusAddr, ":8281", "dumpling API server and pprof addr")
	flags.Uint64VarP(&cfg.Rows, flagRows, "r", UnspecifiedSize, "Split table into chunks of this many rows, default unlimited")
	flags.StringVar(&cfg.Where, flagWhere, "", "Dump only selected records")
	flags.BoolVar(&cfg.EscapeBackslash, flagEscapeBackslash, true, "use backslash to escape special characters")
	flags.StringVar(&cfg.FileType, flagFiletype, FileFormatSQLTextString, "The type of export file (sql/csv)")
	flags.BoolVar(&cfg.NoHeader, flagNoHeader, false, "whether not to dump CSV table header")
	flags.BoolVarP(&cfg.NoSchemas, flagNoSchemas, "m", false, "Do not dump table schemas with the data")
	flags.BoolVarP(&cfg.NoData, flagNoData, "d", false, "Do not dump table data")
	flags.StringVar(&cfg.CsvNullValue, flagCsvNullValue, "\\N", "The null value used when export to csv")
	flags.StringVarP(&cfg.SQL, flagSQL, "S", "", "Dump data with given sql. This argument doesn't support concurrent dump")
	flags.BoolVar(&cfg.DumpEmptyDatabase, flagDumpEmptyDatabase, true, "whether to dump empty database")
	flags.Uint64Var(&cfg.TiDBMemQuotaQuery, flagTidbMemQuotaQuery, DefaultTiDBMemQuotaQuery, "The maximum memory limit for a single SQL statement, in bytes. Default: 32GB")
	flags.StringVar(&cfg.Security.CAPath, flagCA, "", "The path name to the certificate authority file for TLS connection")
	flags.StringVar(&cfg.Security.CertPath, flagCert, "", "The path name to the client certificate file for TLS connection")
	flags.StringVar(&cfg.Security.KeyPath, flagKey, "", "The path name to the client private key file for TLS connection")
	flags.StringVar(&cfg.CsvSeparator, flagCsvSeparator, ",", "The separator for csv files, default ','")
	flags.StringVar(&cfg.CsvDelimiter, flagCsvDelimiter, "\"", "The delimiter for values in csv files, default '\"'")
	flags.BoolVar(&cfg.CompleteInsert, flagCompleteInsert, false, "Use complete INSERT statements that include column names")
	flags.DurationVar(&cfg.ReadTimeout, flagReadTimeout, 15*time.Minute, "I/O read timeout for db connection.")
	_ = flags.MarkHidden(flagReadTimeout)
	flags.BoolVar(&cfg.TransactionalConsistency, flagTransactionalConsistency, true, "Only support transactional consistency")

	storage.DefineFlags(flags)
	flags.StringSliceP(flagTablesList, "T", nil, "Comma delimited table list to dump; must be qualified table names")
	flags.StringP(flagFilesize, "F", "", "The approximate size of output file")
	flags.StringSliceP(flagFilter, "f", []string{"*.*", DefaultTableFilter}, "filter to select which tables to dump")
	flags.Bool(flagCaseSensitive, false, "whether the filter should be case-sensitive")
	flags.String(flagOutputFilenameTemplate, "", "The output filename template (without file extension)")
	flags.StringToString(flagParams, nil, `Extra session variables used while dumping, accepted format: --params "character_set_client=latin1,character_set_connection=latin1"`)
	flags.StringP(flagCompress, "c", "", "Compress output file type, support 'gzip', 'no-compression' now")
	flags.Bool(FlagHelp, false, "Print help message and quit")
	flags.BoolP(FlagVersion, "V", false, "Print Dumpling version")

	cfg.ServerInfo = ServerInfoUnknown
	cfg.SortByPk = true
	cfg.SessionParams = make(map[string]interface{})
	cfg.OutputFileTemplate = DefaultOutputFileTemplate

	return cfg
}

// String returns dumpling's config in json format
func (conf *Config) String() string {
	cfg, err := json.Marshal(conf)
	if err != nil {
		log.Error("fail to marshal config to json", zap.Error(err))
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

// ParseAndAdjust parse flag set and adjust config
func (conf *Config) ParseAndAdjust(args []string) error {
	if err := conf.FlagSet.Parse(args); err != nil {
		return errors.Trace(err)
	}

	if conf.Threads <= 0 {
		return errors.Errorf("--threads is set to %d. It should be greater than 0", conf.Threads)
	}
	if len(conf.CsvSeparator) == 0 {
		return errors.New("--csv-separator is set to \"\". It must not be an empty string")
	}

	tablesList, err := conf.FlagSet.GetStringSlice(flagTablesList)
	if err != nil {
		return errors.Trace(err)
	}
	fileSizeStr, err := conf.FlagSet.GetString(flagFilesize)
	if err != nil {
		return errors.Trace(err)
	}
	filters, err := conf.FlagSet.GetStringSlice(flagFilter)
	if err != nil {
		return errors.Trace(err)
	}
	caseSensitive, err := conf.FlagSet.GetBool(flagCaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}
	outputFilenameFormat, err := conf.FlagSet.GetString(flagOutputFilenameTemplate)
	if err != nil {
		return errors.Trace(err)
	}
	params, err := conf.FlagSet.GetStringToString(flagParams)
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

	if outputFilenameFormat == "" && conf.SQL != "" {
		outputFilenameFormat = DefaultAnonymousOutputFileTemplateText
	}
	tmpl, err := ParseOutputFileTemplate(outputFilenameFormat)
	if err != nil {
		return errors.Errorf("failed to parse output filename template (--output-filename-template '%s')\n", outputFilenameFormat)
	}
	conf.OutputFileTemplate = tmpl

	compressType, err := conf.FlagSet.GetString(flagCompress)
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

	err = conf.BackendOptions.ParseFromFlags(conf.FlagSet)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// ParseFileSize parses file size from tables-list and filter arguments
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

// ParseTableFilter parses table filter from tables-list and filter arguments
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

// ParseCompressType parses compressType string to storage.CompressType
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

func (conf *Config) createExternalStorage(ctx context.Context) (storage.ExternalStorage, error) {
	b, err := storage.ParseBackend(conf.OutputDirPath, &conf.BackendOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	httpClient := http.DefaultClient
	httpClient.Timeout = 30 * time.Second
	maxIdleConnsPerHost := http.DefaultMaxIdleConnsPerHost
	if conf.Threads > maxIdleConnsPerHost {
		maxIdleConnsPerHost = conf.Threads
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
	httpClient.Transport = transport

	return storage.New(ctx, b, &storage.ExternalStorageOptions{
		HTTPClient: httpClient,
	})
}

const (
	// UnspecifiedSize means the filesize/statement-size is unspecified
	UnspecifiedSize = 0
	// DefaultTiDBMemQuotaQuery is the default TiDBMemQuotaQuery size for TiDB
	DefaultTiDBMemQuotaQuery = 32 << 30
	// DefaultStatementSize is the default statement size
	DefaultStatementSize = 1000000
	// TiDBMemQuotaQueryName is the session variable TiDBMemQuotaQuery's name in TiDB
	TiDBMemQuotaQueryName = "tidb_mem_quota_query"
	// DefaultTableFilter is the default exclude table filter. It will exclude all system databases
	DefaultTableFilter = "!/^(mysql|sys|INFORMATION_SCHEMA|PERFORMANCE_SCHEMA|METRICS_SCHEMA|INSPECTION_SCHEMA)$/.*"

	defaultDumpThreads         = 128
	defaultDumpGCSafePointTTL  = 5 * 60
	dumplingServiceSafePointID = "dumpling"
	defaultEtcdDialTimeOut     = 3 * time.Second
)

var (
	gcSafePointVersion = semver.New("4.0.0")
	tableSampleVersion = semver.New("5.0.0")
)

// ServerInfo is the combination of ServerType and ServerInfo
type ServerInfo struct {
	ServerType    ServerType
	ServerVersion *semver.Version
}

// ServerInfoUnknown is the unknown database type to dumpling
var ServerInfoUnknown = ServerInfo{
	ServerType:    ServerTypeUnknown,
	ServerVersion: nil,
}

var (
	versionRegex     = regexp.MustCompile(`^\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
	tidbVersionRegex = regexp.MustCompile(`-[v]?\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
)

// ParseServerInfo parses exported server type and version info from version string
func ParseServerInfo(src string) ServerInfo {
	log.Debug("parse server info", zap.String("server info string", src))
	lowerCase := strings.ToLower(src)
	serverInfo := ServerInfo{}
	switch {
	case strings.Contains(lowerCase, "tidb"):
		serverInfo.ServerType = ServerTypeTiDB
	case strings.Contains(lowerCase, "mariadb"):
		serverInfo.ServerType = ServerTypeMariaDB
	case versionRegex.MatchString(lowerCase):
		serverInfo.ServerType = ServerTypeMySQL
	default:
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

// ServerType represents the type of database to export
type ServerType int8

// String implements Stringer.String
func (s ServerType) String() string {
	if s >= ServerTypeAll {
		return ""
	}
	return serverTypeString[s]
}

const (
	// ServerTypeUnknown represents unknown server type
	ServerTypeUnknown = iota
	// ServerTypeMySQL represents MySQL server type
	ServerTypeMySQL
	// ServerTypeMariaDB represents MariaDB server type
	ServerTypeMariaDB
	// ServerTypeTiDB represents TiDB server type
	ServerTypeTiDB

	// ServerTypeAll represents All server types
	ServerTypeAll
)

var serverTypeString = []string{
	ServerTypeUnknown: "Unknown",
	ServerTypeMySQL:   "MySQL",
	ServerTypeMariaDB: "MariaDB",
	ServerTypeTiDB:    "TiDB",
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
			return errors.Trace(err)
		}
	}
	return nil
}

func registerTLSConfig(conf *Config) error {
	if len(conf.Security.CAPath) > 0 {
		tlsConfig, err := utils.ToTLSConfig(conf.Security.CAPath, conf.Security.CertPath, conf.Security.KeyPath)
		if err != nil {
			return errors.Trace(err)
		}
		err = mysql.RegisterTLSConfig("dumpling-tls-target", tlsConfig)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func validateSpecifiedSQL(conf *Config) error {
	if conf.SQL != "" && conf.Where != "" {
		return errors.New("can't specify both --sql and --where at the same time. Please try to combine them into --sql")
	}
	return nil
}

func validateFileFormat(conf *Config) error {
	conf.FileType = strings.ToLower(conf.FileType)
	switch conf.FileType {
	case FileFormatSQLTextString, FileFormatCSVString:
		return nil
	}
	return errors.Errorf("unknown config.FileType '%s'", conf.FileType)
}
