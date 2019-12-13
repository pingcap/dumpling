package out

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestWrapBackticks(c *C) {
	src := []string{"test1", "`test2", "test3`", "`test4`"}
	exp := []string{"`test1`", "``test2`", "`test3``", "`test4`"}

	for i, s := range src {
		c.Assert(WrapBackticks(s), Equals, exp[i])
	}
}
