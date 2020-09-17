package export

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/c2fo/vfs/v5"
	"github.com/c2fo/vfs/v5/backend"
	"github.com/c2fo/vfs/v5/backend/s3"
	"github.com/c2fo/vfs/v5/vfssimple"
	"github.com/coreos/go-semver/semver"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

type Config struct {
	Databases []string
	Host      string
	User      string
	Port      int
	Password  string `json:"-"`
	Security  struct {
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

	TableFilter        filter.Filter `json:"-"`
	Rows               uint64
	Where              string
	FileType           string
	CompleteInsert     bool
	EscapeBackslash    bool
	DumpEmptyDatabase  bool
	OutputFileTemplate *template.Template `json:"-"`
	SessionParams      map[string]interface{}

	PosAfterConnect bool

	outputLocation     vfs.Location
	outputLocationOnce sync.Once
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
		StatementSize:      UnspecifiedSize,
		OutputDirPath:      ".",
		ServerInfo:         ServerInfoUnknown,
		SortByPk:           true,
		Tables:             nil,
		Snapshot:           "",
		Consistency:        "auto",
		NoViews:            true,
		Rows:               UnspecifiedSize,
		Where:              "",
		FileType:           "SQL",
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
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4", conf.User, conf.Password, conf.Host, conf.Port, db)
	if len(conf.Security.CAPath) > 0 {
		dsn += "&tls=dumpling-tls-target"
	}
	return dsn
}

func (config *Config) OutputLocation() vfs.Location {
	config.outputLocationOnce.Do(func() {
		loc, err := config.doOutputLocation()
		if err != nil {
			panic(err)
		}
		config.outputLocation = loc
	})
	return config.outputLocation
}

func (config *Config) doOutputLocation() (vfs.Location, error) {
	path := config.OutputDirPath
	// TODO support other filesystems
	if strings.HasPrefix(path, "s3://") {
		return config.s3OutputLocation(path)
	} else {
		return config.fileOutputLocation(path)
	}
}

func (config *Config) s3OutputLocation(path string) (vfs.Location, error) {
	client, err := config.awsClient()
	if err != nil {
		return nil, err
	}
	fs := s3.NewFileSystem().WithClient(client)
	backend.Register("s3", fs)
	return vfssimple.NewLocation(ensureTrailingSlash(path))
}

func ensureTrailingSlash(path string) string {
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return path
}

func (config *Config) fileOutputLocation(path string) (vfs.Location, error) {
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(path, "file://") {
		path = "file://" + path
	}
	return vfssimple.NewLocation(ensureTrailingSlash(path))
}

func (config *Config) awsClient() (*awss3.S3, error) {
	cfg := aws.NewConfig()
	endpoint := os.Getenv("DUMPLING_S3_ENDPOINT")
	if endpoint != "" {
		log.Info("using custom S3 endpoint",
			zap.String("endpoint", endpoint))
		cfg = cfg.WithEndpoint(endpoint).
			WithS3ForcePathStyle(true)
	}
	s := session.Must(session.NewSession())
	return awss3.New(s, cfg), nil
}

const (
	UnspecifiedSize            = 0
	DefaultTiDBMemQuotaQuery   = 32 * (1 << 30)
	defaultDumpThreads         = 128
	defaultDumpGCSafePointTTL  = 5 * 60
	dumplingServiceSafePointID = "dumpling"
	defaultEtcdDialTimeOut     = 3 * time.Second
)

var gcSafePointVersion, _ = semver.NewVersion("4.0.0")

type ServerInfo struct {
	ServerType    ServerType
	ServerVersion *semver.Version
}

var ServerInfoUnknown = ServerInfo{
	ServerType:    ServerTypeUnknown,
	ServerVersion: nil,
}

var versionRegex = regexp.MustCompile(`^\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)
var tidbVersionRegex = regexp.MustCompile(`v\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?`)

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
