package export

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	Database string
	Host     string
	User     string
	Port     int
	Password string
	Threads  int

	Logger        Logger
	FileSize      uint64
	OutputDirPath string
	ServerInfo    ServerInfo
	SortByPk      bool
}

func DefaultConfig() *Config {
	return &Config{
		Database:      "",
		Host:          "127.0.0.1",
		User:          "root",
		Port:          3306,
		Password:      "",
		Threads:       4,
		Logger:        &DummyLogger{},
		FileSize:      UnspecifiedSize,
		OutputDirPath: ".",
		ServerInfo:    UnknownServerInfo,
		SortByPk:      false,
	}
}

func (conf *Config) getDSN(db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.User, conf.Password, conf.Host, conf.Port, db)
}

const UnspecifiedSize = 0

type ServerInfo struct {
	ServerType    ServerType
	ServerVersion *ServerVersion
}

var UnknownServerInfo = ServerInfo{
	ServerType:    UnknownServerType,
	ServerVersion: nil,
}

var versionRegex = regexp.MustCompile("^(\\d+\\.){2}\\d+")
var tidbVersionRegex = regexp.MustCompile("v(\\d+\\.){2}\\d+")

func ParseServerInfo(versionStr string) (ServerInfo, error) {
	lowerCase := strings.ToLower(versionStr)
	serverInfo := ServerInfo{}
	if strings.Contains(lowerCase, "tidb") {
		serverInfo.ServerType = TiDBServerType
	} else if strings.Contains(lowerCase, "mariadb") {
		serverInfo.ServerType = MariaDBServerType
	} else if versionRegex.MatchString(lowerCase) {
		serverInfo.ServerType = MySQLServerType
	} else {
		serverInfo.ServerType = UnknownServerType
	}

	var trimmedVersionStr string
	if serverInfo.ServerType == TiDBServerType {
		trimmedVersionStr = tidbVersionRegex.FindString(versionStr)[1:]
	} else {
		trimmedVersionStr = versionRegex.FindString(versionStr)
	}
	versionNums := strings.Split(trimmedVersionStr, ".")
	if len(versionNums) != 3 {
		return serverInfo, nil
	}
	var vs [3]int
	var err error
	for i, s := range versionNums {
		vs[i], err = strconv.Atoi(s)
		if err != nil {
			return serverInfo, err
		}
	}

	serverInfo.ServerVersion = makeServerVersion(vs[0], vs[1], vs[2])
	return serverInfo, nil
}

type ServerType int8

const (
	UnknownServerType = iota
	MySQLServerType
	MariaDBServerType
	TiDBServerType
)

type ServerVersion struct {
	Major int
	Minor int
	Patch int
}

func makeServerVersion(major, minor, patch int) *ServerVersion {
	return &ServerVersion{
		Major: major,
		Minor: minor,
		Patch: patch,
	}
}
