// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"strings"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestCreateExternalStorage(c *C) {
	mockConfig := DefaultConfig()
	loc, err := mockConfig.createExternalStorage(context.Background())
	c.Assert(err, IsNil)
	c.Assert(loc.URI(), Matches, "file:.*")
}

func (s *testConfigSuite) TestParseAndAdjust(c *C) {
	cfg := DefaultConfig()
	c.Assert(cfg.Host, Equals, "127.0.0.1")
	c.Assert(cfg.Password, Equals, "")
	c.Assert(cfg.Port, Equals, 4000)
	c.Assert(cfg.User, Equals, "root")
	c.Assert(cfg.FileSize, Equals, uint64(0))

	extraArgs := "--password 123456 --port 1234"
	cfg.Host = "255.255.255.255"
	cfg.Port = 4321
	cfg.FileSize = uint64(0x12345)

	err := cfg.ParseAndAdjust(strings.Split(extraArgs, " "))
	c.Assert(err, IsNil)

	c.Assert(cfg.Host, Equals, "255.255.255.255") // config manually
	c.Assert(cfg.Password, Equals, "123456")      // config by extraArgs
	c.Assert(cfg.Port, Equals, 1234)              // config both, overwrite by extraArgs
	c.Assert(cfg.User, Equals, "root")            // default value

	c.Assert(cfg.FileSize, Equals, uint64(0x0)) // WARN: extra define flagset can only config by extra args
	extraArgs = "--filesize 1M"                 // otherwise will be overwritten to default value
	err = cfg.ParseAndAdjust(strings.Split(extraArgs, " "))
	c.Assert(err, IsNil)
	c.Assert(cfg.FileSize, Equals, uint64(0x100000))
}
