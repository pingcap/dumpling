package export

import (
	"encoding/json"
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testIRImplSuite{})

type testIRImplSuite struct{}

type simpleRowReceiver struct {
	data []string
}

func newSimpleRowReceiver(length int) *simpleRowReceiver {
	return &simpleRowReceiver{data: make([]string, length)}
}

func (s *simpleRowReceiver) BindAddress(args []interface{}) {
	for i := range args {
		args[i] = &s.data[i]
	}
}

func (s *testIRImplSuite) TestRowIter(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	expectedRows := mock.NewRows([]string{"id"}).
		AddRow("1").
		AddRow("2").
		AddRow("3")
	mock.ExpectQuery("SELECT id from t").WillReturnRows(expectedRows)
	rows, err := db.Query("SELECT id from t")
	c.Assert(err, IsNil)

	iter := newRowIter(rows, 1)
	for i := 0; i < 100; i++ {
		c.Assert(iter.HasNext(), IsTrue)
	}
	res := newSimpleRowReceiver(1)
	c.Assert(iter.Decode(res), IsNil)
	c.Assert(res.data, DeepEquals, []string{"1"})
	iter.Next()
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.Decode(res), IsNil)
	c.Assert(res.data, DeepEquals, []string{"2"})
	iter.Next()
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.Decode(res), IsNil)
	iter.Next()
	c.Assert(res.data, DeepEquals, []string{"3"})
	c.Assert(iter.HasNext(), IsFalse)
}

func (s *testIRImplSuite) TestChunkRowIter(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	twentyBytes := strings.Repeat("x", 20)
	thirtyBytes := strings.Repeat("x", 30)
	expectedRows := mock.NewRows([]string{"a", "b"})
	for i := 0; i < 10; i++ {
		expectedRows.AddRow(twentyBytes, thirtyBytes)
	}
	mock.ExpectQuery("SELECT a, b FROM t").WillReturnRows(expectedRows)
	rows, err := db.Query("SELECT a, b FROM t")
	c.Assert(err, IsNil)

	var (
		testFileSize      uint64 = 200
		testStatementSize uint64 = 101

		expectedSize = [][]uint64{
			{50, 50},
			{100, 100},
			{150, 150},
			{200, 50},
		}
	)

	sqlRowIter := newRowIter(rows, 2)

	res := newSimpleRowReceiver(2)
	wp := newWriterPipe(nil, testFileSize, testStatementSize)

	var resSize [][]uint64
	for sqlRowIter.HasNext() {
		wp.currentStatementSize = 0
		for sqlRowIter.HasNext() {
			c.Assert(sqlRowIter.Decode(res), IsNil)
			sz := uint64(len(res.data[0]) + len(res.data[1]))
			wp.AddFileSize(sz)
			sqlRowIter.Next()
			resSize = append(resSize, []uint64{wp.currentFileSize, wp.currentStatementSize})
			if wp.ShouldSwitchStatement() {
				break
			}
		}
		if wp.ShouldSwitchFile() {
			break
		}
	}

	c.Assert(resSize, DeepEquals, expectedSize)
	c.Assert(sqlRowIter.HasNext(), IsTrue)
	c.Assert(wp.ShouldSwitchFile(), IsTrue)
	c.Assert(wp.ShouldSwitchStatement(), IsTrue)
	rows.Close()
	c.Assert(sqlRowIter.Decode(res), NotNil)
	sqlRowIter.Next()
}

func (s *testIRImplSuite) TestBuildTiDBChunkByRegionWhereCondition(c *C) {
	testCase := []struct {
		json   []string
		expect []string
	}{
		{
			[]string{`{"handle":{"a":"10","b":"2020-01-01 00:00:00"},"table_id":49}`},
			[]string{"(a, b) < ('10', '2020-01-01 00:00:00')", "(a, b) >= ('10', '2020-01-01 00:00:00')"},
		},
		{
			[]string{`{"handle":{"a":"asdf","b":"2020-12-31","c":"6.666"},"table_id":49}`,
				`{"handle":{"a":"xxxxx","b":"2021-12-31","c":"100.00"},"table_id":49}`},
			[]string{"(a, b, c) < ('asdf', '2020-12-31', '6.666')",
				"(a, b, c) < ('asdf', '2020-12-31', '6.666') AND (a, b, c) >= ('xxxxx', '2021-12-31', '100.00')",
				"(a, b, c) >= ('xxxxx', '2021-12-31', '100.00')"},
		},
		{
			[]string{`{"handle":{"a":"\"\"\"","b":"2020-12-31","c":"3.14159"},"table_id":55}`},
			[]string{"(a, b, c) < ('\"\"\"', '2020-12-31', '3.14159')", "(a, b, c) >= ('\"\"\"', '2020-12-31', '3.14159')"},
		},
	}
	for _, tc := range testCase {
		var cutOffPoints []map[string]interface{}
		for _, js := range tc.json {
			var cop map[string]interface{}
			err := json.Unmarshal([]byte(js), &cop)
			c.Assert(err, IsNil)
			cutOffPoints = append(cutOffPoints, cop)
		}
		actual, err := buildTiDBChunkByRegionWhereCondition(cutOffPoints)
		c.Assert(err, IsNil)
		c.Assert(actual, DeepEquals, tc.expect)
	}

}
