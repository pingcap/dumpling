package export

import . "github.com/pingcap/check"

var _ = Suite(&testSqlByteSuite{})

type testSqlByteSuite struct{}

func (s *testSqlByteSuite) TestEscape(c *C) {
	str := `MWQeWw""'\rNmtGxzGp`
	expectStrBackslash := `MWQeWw\"\"\'\\rNmtGxzGp`
	expectStrWithoutBackslash := `MWQeWw""''\\rNmtGxzGp`
	globalEscape = &backslashEscape{}
	c.Assert(globalEscape.Escape(str), Equals, expectStrBackslash)
	globalEscape = &noBackslashEscape{}
	c.Assert(globalEscape.Escape(str), Equals, expectStrWithoutBackslash)
}
