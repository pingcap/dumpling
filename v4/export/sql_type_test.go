package export

import . "github.com/pingcap/check"

var _ = Suite(&testSqlByteSuite{})

type testSqlByteSuite struct{}

func (s *testSqlByteSuite) TestEscape(c *C) {
	str := `MWQeWw""'\rNmtGxzGp`
	expectStrBackslash := `MWQeWw""\'\\rNmtGxzGp`
	expectStrWithoutBackslash := `MWQeWw""''\\rNmtGxzGp`
	escapeBackSlash = true
	c.Assert(escape(str), Equals, expectStrBackslash)
	escapeBackSlash = false
	c.Assert(escape(str), Equals, expectStrWithoutBackslash)
}