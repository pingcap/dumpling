// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"bytes"

	. "github.com/pingcap/check"
)

var _ = Suite(&testSqlByteSuite{})

type testSqlByteSuite struct{}

func (s *testSqlByteSuite) TestEscape(c *C) {
	var bf bytes.Buffer
	str := []byte(`MWQeWw""'\rNmtGxzGp`)
	expectStrBackslash := `MWQeWw\"\"\'\\rNmtGxzGp`
	expectStrWithoutBackslash := `MWQeWw""''\rNmtGxzGp`
	expectStrBackslashDoubleQuote := `MWQeWw""""'\rNmtGxzGp`
	escape(str, &bf, getEscapeQuotation(true, quotationMark))
	c.Assert(bf.String(), Equals, expectStrBackslash)
	bf.Reset()
	escape(str, &bf, getEscapeQuotation(true, doubleQuotationMark))
	c.Assert(bf.String(), Equals, expectStrBackslash)
	bf.Reset()
	escape(str, &bf, getEscapeQuotation(false, quotationMark))
	c.Assert(bf.String(), Equals, expectStrWithoutBackslash)
	bf.Reset()
	escape(str, &bf, getEscapeQuotation(false, doubleQuotationMark))
	c.Assert(bf.String(), Equals, expectStrBackslashDoubleQuote)
}
