// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestCreateExternalStorage(c *C) {
	mockConfig := defaultConfigForTest(c)
	loc, err := mockConfig.createExternalStorage(context.Background())
	c.Assert(err, IsNil)
	c.Assert(loc.URI(), Matches, "file:.*")
}

func (s *testConfigSuite) TestGetDSN(c *C) {
	cfg := &Config{
		Host: "127.0.0.1",
		Port: 4000,
		User: "root",
		Password: "123",
	}

	baseDSN := "root:123@tcp(127.0.0.1:4000)/test?collation=utf8mb4_general_ci&readTimeout=0s&writeTimeout=30s&interpolateParams=true&maxAllowedPacket=0"
	dsn := cfg.GetDSN("test")
	c.Assert(dsn, Equals, baseDSN)

	cfg.Security.CAPath = "/path/test"
	dsn = cfg.GetDSN("test")
	c.Assert(dsn, Equals, baseDSN + "&tls=dumpling-tls-target")

	cfg.Security.CAPath = ""
	cfg.AllowCleartextPasswords = true
	dsn = cfg.GetDSN("test")
	c.Assert(dsn, Equals, baseDSN + "&allowCleartextPasswords=1")

	cfg.TimeZone = "+00:00"
	dsn = cfg.GetDSN("test")
	c.Assert(dsn, Equals, baseDSN + "&allowCleartextPasswords=1&time_zone=%27%2B00%3A00%27")
}
