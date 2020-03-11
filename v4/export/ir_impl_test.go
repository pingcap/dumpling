package export

import (
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

func (s *simpleRowReceiver) ReportSize() uint64 {
	var sum uint64
	for _, datum := range s.data {
		sum += uint64(len(datum))
	}
	return sum
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
	c.Assert(iter.Next(res), IsNil)
	c.Assert(res.data, DeepEquals, []string{"1"})
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.Next(res), IsNil)
	c.Assert(res.data, DeepEquals, []string{"2"})
	c.Assert(iter.HasNext(), IsTrue)
	c.Assert(iter.Next(res), IsNil)
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

	sqlRowIter := SQLRowIter(&fileRowIter{
		rowIter:            newRowIter(rows, 2),
		fileSizeLimit:      testFileSize,
		statementSizeLimit: testStatementSize,
	})

	res := newSimpleRowReceiver(2)

	var resSize [][]uint64
	for sqlRowIter.HasNextSQLRowIter() {
		sqlRowIter = sqlRowIter.NextSQLRowIter()
		fileRowIter, ok := sqlRowIter.(*fileRowIter)
		c.Assert(ok, IsTrue)

		for sqlRowIter.HasNext() {
			c.Assert(sqlRowIter.Next(res), IsNil)
			resSize = append(resSize, []uint64{fileRowIter.currentFileSize, fileRowIter.currentStatementSize})
		}
	}

	c.Assert(resSize, DeepEquals, expectedSize)
	c.Assert(sqlRowIter.HasNextSQLRowIter(), IsFalse)
	c.Assert(sqlRowIter.HasNext(), IsFalse)
	rows.Close()
	c.Assert(sqlRowIter.Next(res), NotNil)
}
