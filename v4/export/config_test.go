package export

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestOutputLocation(c *C) {
	mockConfig := DefaultConfig()
	loc, err := mockConfig.doOutputLocation()
	c.Assert(err, IsNil)
	c.Assert(loc.URI(), Matches, "file:.*")
}
