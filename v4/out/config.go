package out

import "fmt"

type GlobalConfig struct {
	// LineSplitter controls the content of splitter when output.
	LineSplitter string
	// Logger is used to log the output routine.
	Logger Logger
}

var globalConfig = GlobalConfig{
	LineSplitter: "\n",
	Logger:       &DummyLogger{},
}

func GetGlobalConfig() GlobalConfig {
	return globalConfig
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
