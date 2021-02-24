// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package log

import (
	"sync"
	"testing"

	. "github.com/pingcap/check"
	pclog "github.com/pingcap/log"
)

var _ = Suite(&testConcurrentLog{})

type testConcurrentLog struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testConcurrentLog) TestConcurrentLog(c *C) {
	logger, _, _ := pclog.InitLogger(&pclog.Config{
		Level:  "warn",
		Format: "text",
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// test DATA RACE
		for i := 0; i < 10000; i++ {
			Info("test")
		}
		wg.Done()
	}()
	for i := 0; i < 10000; i++ {
		SetAppLogger(logger)
	}
	wg.Wait()
}
