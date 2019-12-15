package dumpling

import (
	"fmt"
	"github.com/pingcap/dumpling/v4/export"
)

type Config struct {
	Database string
	Host     string
	User     string
	Port     int
	Password string
	Threads  int

	LineSplitter  string
	Logger        export.Logger
	OutputSize    int
	OutputDirPath string
}

func DefaultConfig() *Config {
	return &Config{
		Database:      "mysql",
		Host:          "127.0.0.1",
		User:          "root",
		Port:          3306,
		Password:      "",
		Threads:       4,
		LineSplitter:  "\n",
		Logger:        nil,
		OutputSize:    export.UnspecifiedSize,
		OutputDirPath: ".",
	}
}

func (conf *Config) getDSN(db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.User, conf.Password, conf.Host, conf.Port, db)
}

func extractOutputConfig(conf *Config) *export.Config {
	return &export.Config{
		LineSplitter:  conf.LineSplitter,
		Logger:        conf.Logger,
		OutputSize:    conf.OutputSize,
		OutputDirPath: conf.OutputDirPath,
	}
}
