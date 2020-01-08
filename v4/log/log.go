package log

import (
	"log"

	"go.uber.org/zap"
)

var globalLogger *zap.Logger

func init() {
	logger, err := zap.NewDevelopment(zap.AddCaller())
	if err != nil {
		log.Fatalf("unable to init zap.Logger: %s", err.Error())
	}
	globalLogger = logger
	logger.Sugar()
}

// Zap returns the global dumpling logger.
func Zap() *zap.Logger {
	return globalLogger
}
