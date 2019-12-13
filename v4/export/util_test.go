package export

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestWrapBackticks(c *C) {
	src := []string{"test1", "`test2", "test3`", "`test4`"}
	exp := []string{"`test1`", "``test2`", "`test3``", "`test4`"}

	for i, s := range src {
		c.Assert(wrapBackticks(s), Equals, exp[i])
	}
}

func (s *testUtilSuite) TestHandleNulls(c *C) {
	src := []string{"255", "", "25535", "computer_science", "male"}
	exp := []string{"255", "NULL", "25535", "computer_science", "male"}
	c.Assert(handleNulls(src), DeepEquals, exp)
}
