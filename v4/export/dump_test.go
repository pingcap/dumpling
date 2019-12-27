package export

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testDumpSuite{})

type testDumpSuite struct{}

func (s *testDumpSuite) SetUpSuite(c *C) {
	rand.Seed(time.Now().Unix())
}

func (s *testDumpSuite) TestDetectServerInfo(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mkVer := makeServerVersion
	data := [][]interface{}{
		{"8.0.18", MySQLServerType, mkVer(8, 0, 18)},
		{"10.4.10-MariaDB-1:10.4.10+maria~bionic", MariaDBServerType, mkVer(10, 4, 10)},
		{"5.7.25-TiDB-v4.0.0-alpha-1263-g635f2e1af", TiDBServerType, mkVer(4, 0, 0)},
		{"5.7.25-TiDB-v3.0.7-58-g6adce2367", TiDBServerType, mkVer(3, 0, 7)},
	}

	for _, datum := range data {
		rows := sqlmock.NewRows([]string{"version"}).
			AddRow(datum[0])
		mock.ExpectQuery("SELECT version()").WillReturnRows(rows)

		info, err := detectServerInfo(db)
		c.Assert(err, IsNil)
		c.Assert(info.ServerType, Equals, ServerType(datum[1].(int)))
		c.Assert(info.ServerVersion, DeepEquals, datum[2].(*ServerVersion), Commentf("ServerType: %v", info.ServerType))
		c.Assert(mock.ExpectationsWereMet(), IsNil)
	}
}

func (s *testDumpSuite) TestBuildSelectAllQuery(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	const database, table = "test", "t"
	getPkQuery := fmt.Sprintf(PrimaryKeyQuery, database, table)
	const needSortedByPK = true
	const noNeedSortedByPK = false
	const isTiDB = true
	const notTiDB = false
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
		{1, needSortedByPK, isTiDB, noPriKeyInTable, shouldReturn, orderByTiDBRowIDQuery},
		{2, needSortedByPK, isTiDB, hasPriKeyInTable, shouldReturn, orderByPkQuery},
		{3, needSortedByPK, notTiDB, noPriKeyInTable, shouldReturn, plainQuery},
		{4, needSortedByPK, notTiDB, hasPriKeyInTable, shouldReturn, orderByPkQuery},
		{5, noNeedSortedByPK, isTiDB, noPriKeyInTable, shouldReturn, plainQuery},
		{6, noNeedSortedByPK, isTiDB, hasPriKeyInTable, shouldReturn, plainQuery},
		{7, noNeedSortedByPK, notTiDB, noPriKeyInTable, shouldReturn, plainQuery},
		{8, noNeedSortedByPK, notTiDB, hasPriKeyInTable, shouldReturn, plainQuery},
	}

	for _, n := range cases {
		tag, orderByPk, isTiDBType := n[0].(int), n[1].(bool), n[2].(bool)
		rowsMaker, result := n[3].(func() *sqlmock.Rows), n[5].(string)

		mockConf.SortByPk = orderByPk
		if mockConf.SortByPk {
			mock.ExpectQuery(getPkQuery).WillReturnRows(rowsMaker())
		}
		if isTiDBType {
			mockConf.ServerInfo.ServerType = TiDBServerType
		} else {
			mockConf.ServerInfo.ServerType = randDBType([]ServerType{UnknownServerType, MySQLServerType, MariaDBServerType})
		}

		q, err := buildSelectAllQuery(mockConf, db, database, table)
		cmt := Commentf("The test case number is: %d", tag)
		c.Assert(err, IsNil, cmt)
		c.Assert(q, Equals, result, cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
	}
}

func randDBType(ss []ServerType) ServerType {
	return ss[rand.Intn(len(ss))]
}
