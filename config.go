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

	Logger        export.Logger
	OutputSize    int
	OutputDirPath string
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
		OutputSize:    export.UnspecifiedSize,
		OutputDirPath: ".",
	}
}

func (conf *Config) getDSN(db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.User, conf.Password, conf.Host, conf.Port, db)
}

func extractOutputConfig(conf *Config) *export.Config {
	return &export.Config{
		Logger:        conf.Logger,
		OutputSize:    conf.OutputSize,
		OutputDirPath: conf.OutputDirPath,
	}
}

type DummyLogger struct{}

func (d *DummyLogger) Debug(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Info(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Warn(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Error(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}
