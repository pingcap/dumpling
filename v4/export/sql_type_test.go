// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSQLByteSuite{})

type testSQLByteSuite struct{}

func (s *testSQLByteSuite) TestEscape(c *C) {
	var bf bytes.Buffer
	str := []byte(`MWQeWw""'\rNmtGxzGp`)
	expectStrBackslash := `MWQeWw\"\"\'\\rNmtGxzGp`
	expectStrWithoutBackslash := `MWQeWw""''\rNmtGxzGp`
	expectStrBackslashDoubleQuote := `MWQeWw""""'\rNmtGxzGp`
	escapeSQL(str, &bf, true)
	c.Assert(bf.String(), Equals, expectStrBackslash)
	bf.Reset()
	escapeSQL(str, &bf, false)
	c.Assert(bf.String(), Equals, expectStrWithoutBackslash)
	bf.Reset()
	escapeCSV(str, &bf, true, []byte(`"`), []byte(`,`))
	c.Assert(bf.String(), Equals, expectStrBackslash)
	bf.Reset()
	escapeCSV(str, &bf, false, []byte(`"`), []byte(`,`))
	c.Assert(bf.String(), Equals, expectStrBackslashDoubleQuote)
	bf.Reset()

	str = []byte(`a|*|b"cd`)
	expectedStrWithDelimiter := `a|*|b""cd`
	expectedStrWithoutDelimiter := `a\|*\|b"cd`

	escapeCSV(str, &bf, false, []byte(`"`), []byte(`,`))
	c.Assert(bf.String(), Equals, expectedStrWithDelimiter)
	bf.Reset()

	escapeCSV(str, &bf, false, []byte(``), []byte(`|*|`))
	c.Assert(bf.String(), Equals, expectedStrWithoutDelimiter)
	bf.Reset()
}
