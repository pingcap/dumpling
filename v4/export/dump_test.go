package export

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
)

var _ = Suite(&testDumpSuite{})

type testDumpSuite struct{}

func (s *testDumpSuite) TestDetectServerInfo(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mkVer := makeVersion
	data := [][]interface{}{
		{1, "8.0.18", ServerTypeMySQL, mkVer(8, 0, 18, "")},
		{2, "10.4.10-MariaDB-1:10.4.10+maria~bionic", ServerTypeMariaDB, mkVer(10, 4, 10, "MariaDB-1")},
		{3, "5.7.25-TiDB-v4.0.0-alpha-1263-g635f2e1af", ServerTypeTiDB, mkVer(4, 0, 0, "alpha-1263-g635f2e1af")},
		{4, "5.7.25-TiDB-v3.0.7-58-g6adce2367", ServerTypeTiDB, mkVer(3, 0, 7, "58-g6adce2367")},
		{5, "invalid version", ServerTypeUnknown, (*semver.Version)(nil)},
	}
	dec := func(d []interface{}) (tag int, verStr string, tp ServerType, v *semver.Version) {
		return d[0].(int), d[1].(string), ServerType(d[2].(int)), d[3].(*semver.Version)
	}

	for _, datum := range data {
		tag, r, serverTp, expectVer := dec(datum)
		cmt := Commentf("test case number: %d", tag)

		rows := sqlmock.NewRows([]string{"version"}).AddRow(r)
		mock.ExpectQuery("SELECT version()").WillReturnRows(rows)

		info, err := detectServerInfo(db)
		c.Assert(err, IsNil, cmt)
		c.Assert(info.ServerType, Equals, serverTp, cmt)
		c.Assert(info.ServerVersion == nil, Equals, expectVer == nil, cmt)
		if info.ServerVersion == nil {
			c.Assert(expectVer, IsNil, cmt)
		} else {
			c.Assert(info.ServerVersion.Equal(*expectVer), IsTrue)
		}
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}
}

func (s *testDumpSuite) TestBuildSelectAllQuery(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	const database, table = "test", "t"
	const needSortedByPK = true
	const noNeedSortedByPK = false
	noPriKeyInTable := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"COLUMN_NAME"})
	}
	hasPriKeyInTable := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("id")
	}

	plainQuery := "SELECT * FROM test.t"
	orderByPkQuery := "SELECT * FROM test.t ORDER BY id"
	orderByTiDBRowIDQuery := "SELECT * FROM test.t ORDER BY _tidb_rowid"

	mockConf := DefaultConfig()
	const shouldReturn = "placeholder"
	cases := [][]interface{}{
		// example:
		// if we need to sort the output by primary key and
		//    the server is TiDB and
		//    there is no primary key in the testing table,
		// then `buildSelectAllQuery` should return a "select * from t order by _tidb_rowid".
		{1, needSortedByPK, ServerTypeTiDB, noPriKeyInTable, shouldReturn, orderByTiDBRowIDQuery},
		{2, needSortedByPK, ServerTypeTiDB, hasPriKeyInTable, shouldReturn, orderByPkQuery},
		{3, needSortedByPK, ServerTypeUnknown, noPriKeyInTable, shouldReturn, plainQuery},
		{4, needSortedByPK, ServerTypeUnknown, hasPriKeyInTable, shouldReturn, orderByPkQuery},
		{5, noNeedSortedByPK, ServerTypeTiDB, noPriKeyInTable, shouldReturn, plainQuery},
		{6, noNeedSortedByPK, ServerTypeTiDB, hasPriKeyInTable, shouldReturn, plainQuery},
		{7, noNeedSortedByPK, ServerTypeUnknown, noPriKeyInTable, shouldReturn, plainQuery},
		{8, noNeedSortedByPK, ServerTypeUnknown, hasPriKeyInTable, shouldReturn, plainQuery},
	}
	dec := func(d []interface{}) (tag int, needOrderByPk bool, tp ServerType, rowMaker func()*sqlmock.Rows, result string) {
		return d[0].(int), d[1].(bool), ServerType(d[2].(int)), d[3].(func() *sqlmock.Rows), d[5].(string)
	}

	for _, n := range cases {
		tag, orderByPk, serverType, rowsMaker, result := dec(n)
		cmt := Commentf("test case number: %d", tag)
		mockConf.SortByPk = orderByPk
		mockConf.ServerInfo.ServerType = serverType
		if mockConf.SortByPk {
			mock.ExpectPrepare("SELECT column_name FROM information_schema.columns").
				ExpectQuery().WithArgs(database, table).
				WillReturnRows(rowsMaker())
		}

		q, err := buildSelectAllQuery(mockConf, db, database, table)
		c.Assert(err, IsNil, cmt)
		c.Assert(q, Equals, result, cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
	}
}

func makeVersion(major, minor, patch int64, preRelease string) *semver.Version {
	return &semver.Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: semver.PreRelease(preRelease),
		Metadata:   "",
	}
}