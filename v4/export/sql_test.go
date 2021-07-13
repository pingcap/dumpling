// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/siddontang/go-mysql/mysql"
)

var _ = Suite(&testSQLSuite{})

type testSQLSuite struct{}

func (s *testSQLSuite) TestDetectServerInfo(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mkVer := makeVersion
	data := [][]interface{}{
		{1, "8.0.18", ServerTypeMySQL, mkVer(8, 0, 18, "")},
		{2, "10.4.10-MariaDB-1:10.4.10+maria~bionic", ServerTypeMariaDB, mkVer(10, 4, 10, "MariaDB-1")},
		{3, "5.7.25-TiDB-v4.0.0-alpha-1263-g635f2e1af", ServerTypeTiDB, mkVer(4, 0, 0, "alpha-1263-g635f2e1af")},
		{4, "5.7.25-TiDB-v3.0.7-58-g6adce2367", ServerTypeTiDB, mkVer(3, 0, 7, "58-g6adce2367")},
		{5, "5.7.25-TiDB-3.0.6", ServerTypeTiDB, mkVer(3, 0, 6, "")},
		{6, "invalid version", ServerTypeUnknown, (*semver.Version)(nil)},
	}
	dec := func(d []interface{}) (tag int, verStr string, tp ServerType, v *semver.Version) {
		return d[0].(int), d[1].(string), ServerType(d[2].(int)), d[3].(*semver.Version)
	}

	for _, datum := range data {
		tag, r, serverTp, expectVer := dec(datum)
		cmt := Commentf("test case number: %d", tag)

		rows := sqlmock.NewRows([]string{"version"}).AddRow(r)
		mock.ExpectQuery("SELECT version()").WillReturnRows(rows)

		verStr, err := SelectVersion(db)
		c.Assert(err, IsNil, cmt)
		info := ParseServerInfo(tcontext.Background(), verStr)
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

func (s *testSQLSuite) TestBuildSelectAllQuery(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	mockConf := defaultConfigForTest(c)
	mockConf.SortByPk = true

	// Test TiDB server.
	mockConf.ServerInfo.ServerType = ServerTypeTiDB

	// _tidb_rowid is available.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

	selectedField, _, err := buildSelectField(conn, "test", "t", false)
	c.Assert(err, IsNil)
	q := buildSelectQuery("test", "t", selectedField, "", "", orderByClause)
	c.Assert(q, Equals, "SELECT * FROM `test`.`t` ORDER BY `_tidb_rowid`")

	// _tidb_rowid is unavailable, or PKIsHandle.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnError(errors.New(`1054, "Unknown column '_tidb_rowid' in 'field list'"`))

	mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
		WithArgs("test", "t").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))

	orderByClause, err = buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)

	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

	selectedField, _, err = buildSelectField(conn, "test", "t", false)
	c.Assert(err, IsNil)
	q = buildSelectQuery("test", "t", selectedField, "", "", orderByClause)
	c.Assert(q, Equals, "SELECT * FROM `test`.`t` ORDER BY `id`")
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// Test other servers.
	otherServers := []ServerType{ServerTypeUnknown, ServerTypeMySQL, ServerTypeMariaDB}

	// Test table with primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))
		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)

		mock.ExpectQuery("SELECT COLUMN_NAME").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

		selectedField, _, err = buildSelectField(conn, "test", "t", false)
		c.Assert(err, IsNil)
		q = buildSelectQuery("test", "t", selectedField, "", "", orderByClause)
		c.Assert(q, Equals, "SELECT * FROM `test`.`t` ORDER BY `id`", cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}

	// Test table without primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)

		mock.ExpectQuery("SELECT COLUMN_NAME").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

		selectedField, _, err = buildSelectField(conn, "test", "t", false)
		c.Assert(err, IsNil)
		q := buildSelectQuery("test", "t", selectedField, "", "", orderByClause)
		c.Assert(q, Equals, "SELECT * FROM `test`.`t`", cmt)
		err = mock.ExpectationsWereMet()
		c.Assert(err, IsNil, cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil)
	}

	// Test when config.SortByPk is disabled.
	mockConf.SortByPk = false
	for tp := ServerTypeUnknown; tp < ServerTypeAll; tp++ {
		mockConf.ServerInfo.ServerType = ServerType(tp)
		cmt := Commentf("current server type: ", tp)

		mock.ExpectQuery("SELECT COLUMN_NAME").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

		selectedField, _, err := buildSelectField(conn, "test", "t", false)
		c.Assert(err, IsNil)
		q := buildSelectQuery("test", "t", selectedField, "", "", "")
		c.Assert(q, Equals, "SELECT * FROM `test`.`t`", cmt)
		c.Assert(mock.ExpectationsWereMet(), IsNil, cmt)
	}
}

func (s *testSQLSuite) TestBuildOrderByClause(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	mockConf := defaultConfigForTest(c)
	mockConf.SortByPk = true

	// Test TiDB server.
	mockConf.ServerInfo.ServerType = ServerTypeTiDB

	// _tidb_rowid is available.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(orderByClause, Equals, orderByTiDBRowID)

	// _tidb_rowid is unavailable, or PKIsHandle.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnError(errors.New(`1054, "Unknown column '_tidb_rowid' in 'field list'"`))

	mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
		WithArgs("test", "t").
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))

	orderByClause, err = buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(orderByClause, Equals, "ORDER BY `id`")

	// Test other servers.
	otherServers := []ServerType{ServerTypeUnknown, ServerTypeMySQL, ServerTypeMariaDB}

	// Test table with primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id"))
		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "ORDER BY `id`", cmt)
	}

	// Test table with joint primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow("id").AddRow("name"))
		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "ORDER BY `id`,`name`", cmt)
	}

	// Test table without primary key.
	for _, serverTp := range otherServers {
		mockConf.ServerInfo.ServerType = serverTp
		cmt := Commentf("server type: %s", serverTp)
		mock.ExpectQuery("SELECT column_name FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("test", "t").
			WillReturnRows(sqlmock.NewRows([]string{"column_name"}))

		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "", cmt)
	}

	// Test when config.SortByPk is disabled.
	mockConf.SortByPk = false
	for tp := ServerTypeUnknown; tp < ServerTypeAll; tp++ {
		mockConf.ServerInfo.ServerType = ServerType(tp)
		cmt := Commentf("current server type: ", tp)

		orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
		c.Assert(err, IsNil, cmt)
		c.Assert(orderByClause, Equals, "", cmt)
	}
}

func (s *testSQLSuite) TestBuildSelectField(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	// generate columns not found
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", ""))

	selectedField, _, err := buildSelectField(conn, "test", "t", false)
	c.Assert(selectedField, Equals, "*")
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// user assigns completeInsert
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).AddRow("id", "").
			AddRow("name", "").AddRow("quo`te", ""))

	selectedField, _, err = buildSelectField(conn, "test", "t", true)
	c.Assert(selectedField, Equals, "`id`,`name`,`quo``te`")
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// found generate columns, rest columns is `id`,`name`
	mock.ExpectQuery("SELECT COLUMN_NAME").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "extra"}).
			AddRow("id", "").AddRow("name", "").AddRow("quo`te", "").AddRow("generated", "VIRTUAL GENERATED"))

	selectedField, _, err = buildSelectField(conn, "test", "t", false)
	c.Assert(selectedField, Equals, "`id`,`name`,`quo``te`")
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testSQLSuite) TestParseSnapshotToTSO(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	snapshot := "2020/07/18 20:31:50"
	var unixTimeStamp uint64 = 1595075510
	// generate columns valid snapshot
	mock.ExpectQuery(`SELECT unix_timestamp(?)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{`unix_timestamp("2020/07/18 20:31:50")`}).AddRow(1595075510))
	tso, err := parseSnapshotToTSO(db, snapshot)
	c.Assert(err, IsNil)
	c.Assert(tso, Equals, (unixTimeStamp<<18)*1000+1)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// generate columns not valid snapshot
	mock.ExpectQuery(`SELECT unix_timestamp(?)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{`unix_timestamp("XXYYZZ")`}).AddRow(nil))
	tso, err = parseSnapshotToTSO(db, "XXYYZZ")
	c.Assert(err, ErrorMatches, "snapshot XXYYZZ format not supported. please use tso or '2006-01-02 15:04:05' format time")
	c.Assert(tso, Equals, uint64(0))
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testSQLSuite) TestShowCreateView(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	mock.ExpectQuery("SHOW FIELDS FROM `test`.`v`").
		WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("a", "int(11)", "YES", nil, "NULL", nil))

	mock.ExpectQuery("SHOW CREATE VIEW `test`.`v`").
		WillReturnRows(sqlmock.NewRows([]string{"View", "Create View", "character_set_client", "collation_connection"}).
			AddRow("v", "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`", "utf8", "utf8_general_ci"))

	createTableSQL, createViewSQL, err := ShowCreateView(conn, "test", "v")
	c.Assert(err, IsNil)
	c.Assert(createTableSQL, Equals, "CREATE TABLE `v`(\n`a` int\n)ENGINE=MyISAM;\n")
	c.Assert(createViewSQL, Equals, "DROP TABLE IF EXISTS `v`;\nDROP VIEW IF EXISTS `v`;\nSET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\nSET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\nSET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\nSET character_set_client = utf8;\nSET character_set_results = utf8;\nSET collation_connection = utf8_general_ci;\nCREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`;\nSET character_set_client = @PREV_CHARACTER_SET_CLIENT;\nSET character_set_results = @PREV_CHARACTER_SET_RESULTS;\nSET collation_connection = @PREV_COLLATION_CONNECTION;\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testSQLSuite) TestGetSuitableRows(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx, cancel := tcontext.Background().WithCancel()
	defer cancel()
	const (
		query    = "select AVG_ROW_LENGTH from INFORMATION_SCHEMA.TABLES where table_schema=\\? and table_name=\\?;"
		database = "foo"
		table    = "bar"
	)

	testCases := []struct {
		avgRowLength uint64
		expectedRows uint64
		returnErr    error
	}{
		{
			32,
			200000,
			sql.ErrNoRows,
		},
		{
			0,
			200000,
			nil,
		},
		{
			32,
			1000000,
			nil,
		},
		{
			1024,
			131072,
			nil,
		},
		{
			4096,
			32768,
			nil,
		},
	}
	for _, testCase := range testCases {
		if testCase.returnErr == nil {
			mock.ExpectQuery(query).WithArgs(database, table).
				WillReturnRows(sqlmock.NewRows([]string{"AVG_ROW_LENGTH"}).
					AddRow(testCase.avgRowLength))
		} else {
			mock.ExpectQuery(query).WithArgs(database, table).
				WillReturnError(testCase.returnErr)
		}
		rows := GetSuitableRows(tctx, conn, database, table)
		c.Assert(rows, Equals, testCase.expectedRows)
	}
}

func (s *testSQLSuite) TestBuildTableSampleQueries(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	d := &Dumper{
		tctx:                      tctx,
		conf:                      DefaultConfig(),
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	d.conf.ServerInfo = ServerInfo{
		HasTiKV:       true,
		ServerType:    ServerTypeTiDB,
		ServerVersion: tableSampleVersion,
	}

	const (
		database = "foo"
		table    = "bar"
	)

	testCases := []struct {
		handleColNames       []string
		handleColTypes       []string
		handleVals           [][]driver.Value
		expectedWhereClauses []string
		hasTiDBRowID         bool
	}{
		{
			[]string{},
			[]string{},
			[][]driver.Value{},
			nil,
			false,
		},
		{
			[]string{"a"},
			[]string{"bigint"},
			[][]driver.Value{{1}},
			[]string{"`a`<1", "`a`>=1"},
			false,
		},
		// check whether dumpling can turn to dump whole table
		{
			[]string{"a"},
			[]string{"bigint"},
			[][]driver.Value{},
			nil,
			false,
		},
		// check whether dumpling can turn to dump whole table
		{
			[]string{"_tidb_rowid"},
			[]string{"bigint"},
			[][]driver.Value{},
			nil,
			true,
		},
		{
			[]string{"_tidb_rowid"},
			[]string{"bigint"},
			[][]driver.Value{{1}},
			[]string{"`_tidb_rowid`<1", "`_tidb_rowid`>=1"},
			true,
		},
		{
			[]string{"a"},
			[]string{"bigint"},
			[][]driver.Value{
				{1},
				{2},
				{3},
			},
			[]string{"`a`<1", "`a`>=1 and `a`<2", "`a`>=2 and `a`<3", "`a`>=3"},
			false,
		},
		{
			[]string{"a", "b"},
			[]string{"bigint", "bigint"},
			[][]driver.Value{{1, 2}},
			[]string{"`a`<1 or(`a`=1 and `b`<2)", "`a`>1 or(`a`=1 and `b`>=2)"},
			false,
		},
		{
			[]string{"a", "b"},
			[]string{"bigint", "bigint"},
			[][]driver.Value{
				{1, 2},
				{3, 4},
				{5, 6},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)",
				"(`a`>1 and `a`<3)or(`a`=1 and(`b`>=2))or(`a`=3 and(`b`<4))",
				"(`a`>3 and `a`<5)or(`a`=3 and(`b`>=4))or(`a`=5 and(`b`<6))",
				"`a`>5 or(`a`=5 and `b`>=6)",
			},
			false,
		},
		{
			[]string{"a", "b", "c"},
			[]string{"bigint", "bigint", "bigint"},
			[][]driver.Value{
				{1, 2, 3},
				{4, 5, 6},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"(`a`>1 and `a`<4)or(`a`=1 and(`b`>2 or(`b`=2 and `c`>=3)))or(`a`=4 and(`b`<5 or(`b`=5 and `c`<6)))",
				"`a`>4 or(`a`=4 and `b`>5)or(`a`=4 and `b`=5 and `c`>=6)",
			},
			false,
		},
		{
			[]string{"a", "b", "c"},
			[]string{"bigint", "bigint", "bigint"},
			[][]driver.Value{
				{1, 2, 3},
				{1, 4, 5},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"`a`=1 and((`b`>2 and `b`<4)or(`b`=2 and(`c`>=3))or(`b`=4 and(`c`<5)))",
				"`a`>1 or(`a`=1 and `b`>4)or(`a`=1 and `b`=4 and `c`>=5)",
			},
			false,
		},
		{
			[]string{"a", "b", "c"},
			[]string{"bigint", "bigint", "bigint"},
			[][]driver.Value{
				{1, 2, 3},
				{1, 2, 8},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"`a`=1 and `b`=2 and(`c`>=3 and `c`<8)",
				"`a`>1 or(`a`=1 and `b`>2)or(`a`=1 and `b`=2 and `c`>=8)",
			},
			false,
		},
		// special case: avoid return same samples
		{
			[]string{"a", "b", "c"},
			[]string{"bigint", "bigint", "bigint"},
			[][]driver.Value{
				{1, 2, 3},
				{1, 2, 3},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)",
				"false",
				"`a`>1 or(`a`=1 and `b`>2)or(`a`=1 and `b`=2 and `c`>=3)",
			},
			false,
		},
		// special case: numbers has bigger lexicographically order but lower number
		{
			[]string{"a", "b", "c"},
			[]string{"bigint", "bigint", "bigint"},
			[][]driver.Value{
				{12, 2, 3},
				{111, 4, 5},
			},
			[]string{
				"`a`<12 or(`a`=12 and `b`<2)or(`a`=12 and `b`=2 and `c`<3)",
				"(`a`>12 and `a`<111)or(`a`=12 and(`b`>2 or(`b`=2 and `c`>=3)))or(`a`=111 and(`b`<4 or(`b`=4 and `c`<5)))", // should return sql correctly
				"`a`>111 or(`a`=111 and `b`>4)or(`a`=111 and `b`=4 and `c`>=5)",
			},
			false,
		},
		// test string fields
		{
			[]string{"a", "b", "c"},
			[]string{"bigint", "bigint", "varchar"},
			[][]driver.Value{
				{1, 2, "3"},
				{1, 4, "5"},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<'3')",
				"`a`=1 and((`b`>2 and `b`<4)or(`b`=2 and(`c`>='3'))or(`b`=4 and(`c`<'5')))",
				"`a`>1 or(`a`=1 and `b`>4)or(`a`=1 and `b`=4 and `c`>='5')",
			},
			false,
		},
		{
			[]string{"a", "b", "c", "d"},
			[]string{"bigint", "bigint", "bigint", "bigint"},
			[][]driver.Value{
				{1, 2, 3, 4},
				{5, 6, 7, 8},
			},
			[]string{
				"`a`<1 or(`a`=1 and `b`<2)or(`a`=1 and `b`=2 and `c`<3)or(`a`=1 and `b`=2 and `c`=3 and `d`<4)",
				"(`a`>1 and `a`<5)or(`a`=1 and(`b`>2 or(`b`=2 and `c`>3)or(`b`=2 and `c`=3 and `d`>=4)))or(`a`=5 and(`b`<6 or(`b`=6 and `c`<7)or(`b`=6 and `c`=7 and `d`<8)))",
				"`a`>5 or(`a`=5 and `b`>6)or(`a`=5 and `b`=6 and `c`>7)or(`a`=5 and `b`=6 and `c`=7 and `d`>=8)",
			},
			false,
		},
	}
	transferHandleValStrings := func(handleColTypes []string, handleVals [][]driver.Value) [][]string {
		handleValStrings := make([][]string, 0, len(handleVals))
		for _, handleVal := range handleVals {
			handleValString := make([]string, 0, len(handleVal))
			for i, val := range handleVal {
				rec := colTypeRowReceiverMap[strings.ToUpper(handleColTypes[i])]()
				var valStr string
				switch rec.(type) {
				case *SQLTypeString:
					valStr = fmt.Sprintf("'%s'", val)
				case *SQLTypeBytes:
					valStr = fmt.Sprintf("x'%x'", val)
				case *SQLTypeNumber:
					valStr = fmt.Sprintf("%d", val)
				}
				handleValString = append(handleValString, valStr)
			}
			handleValStrings = append(handleValStrings, handleValString)
		}
		return handleValStrings
	}

	for caseID, testCase := range testCases {
		c.Log(fmt.Sprintf("case #%d", caseID))
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes
		handleVals := testCase.handleVals
		handleValStrings := transferHandleValStrings(handleColTypes, handleVals)

		// Test build whereClauses
		whereClauses := buildWhereClauses(handleColNames, handleValStrings)
		c.Assert(whereClauses, DeepEquals, testCase.expectedWhereClauses)

		// Test build tasks through table sample
		if len(handleColNames) > 0 {
			taskChan := make(chan Task, 128)
			quotaCols := make([]string, 0, len(handleColNames))
			for _, col := range quotaCols {
				quotaCols = append(quotaCols, wrapBackTicks(col))
			}
			selectFields := strings.Join(quotaCols, ",")
			meta := &tableMeta{
				database:      database,
				table:         table,
				selectedField: selectFields,
				specCmts: []string{
					"/*!40101 SET NAMES binary*/;",
				},
			}

			if testCase.hasTiDBRowID {
				mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
					WillReturnResult(sqlmock.NewResult(0, 0))
			} else {
				mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
					WillReturnError(&mysql.MyError{
						Code:    mysql.ER_BAD_FIELD_ERROR,
						State:   "42S22",
						Message: "Unknown column '_tidb_rowid' in 'field list'",
					})
				rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE"})
				for i := range handleColNames {
					rows.AddRow(handleColNames[i], handleColTypes[i])
				}
				mock.ExpectQuery("SELECT c.COLUMN_NAME, DATA_TYPE FROM").WithArgs(database, table).WillReturnRows(rows)
			}

			rows := sqlmock.NewRows(handleColNames)
			for _, handleVal := range handleVals {
				rows.AddRow(handleVal...)
			}
			mock.ExpectQuery(fmt.Sprintf("SELECT .* FROM `%s`.`%s` TABLESAMPLE REGIONS", database, table)).WillReturnRows(rows)

			rows = sqlmock.NewRows([]string{"COLUMN_NAME", "EXTRA"})
			for _, handleCol := range handleColNames {
				rows.AddRow(handleCol, "")
			}
			mock.ExpectQuery("SELECT COLUMN_NAME,EXTRA FROM INFORMATION_SCHEMA.COLUMNS").WithArgs(database, table).
				WillReturnRows(rows)
			// special case, no value found, will scan whole table and try build order clause
			if len(handleVals) == 0 {
				mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
					WillReturnResult(sqlmock.NewResult(0, 0))
			}

			c.Assert(d.concurrentDumpTable(tctx, conn, meta, taskChan), IsNil)
			c.Assert(mock.ExpectationsWereMet(), IsNil)
			orderByClause := buildOrderByClauseString(handleColNames)

			checkQuery := func(i int, query string) {
				task := <-taskChan
				taskTableData, ok := task.(*TaskTableData)
				c.Assert(ok, IsTrue)
				c.Assert(taskTableData.ChunkIndex, Equals, i)
				data, ok := taskTableData.Data.(*tableData)
				c.Assert(ok, IsTrue)
				c.Assert(data.query, Equals, query)
			}

			// special case, no value found
			if len(handleVals) == 0 {
				orderByClause = orderByTiDBRowID
				query := buildSelectQuery(database, table, "*", "", "", orderByClause)
				checkQuery(0, query)
				continue
			}

			for i, w := range testCase.expectedWhereClauses {
				query := buildSelectQuery(database, table, "*", "", buildWhereCondition(d.conf, w), orderByClause)
				checkQuery(i, query)
			}
		}
	}
}

func (s *testSQLSuite) TestBuildPartitionClauses(c *C) {
	const (
		dbName        = "test"
		tbName        = "t"
		fields        = "*"
		partition     = "p0"
		where         = "WHERE a > 10"
		orderByClause = "ORDER BY a"
	)
	testCases := []struct {
		partition     string
		where         string
		orderByClause string
		expectedQuery string
	}{
		{
			"",
			"",
			"",
			"SELECT * FROM `test`.`t`",
		},
		{
			partition,
			"",
			"",
			"SELECT * FROM `test`.`t` PARTITION(`p0`)",
		},
		{
			partition,
			where,
			"",
			"SELECT * FROM `test`.`t` PARTITION(`p0`) WHERE a > 10",
		},
		{
			partition,
			"",
			orderByClause,
			"SELECT * FROM `test`.`t` PARTITION(`p0`) ORDER BY a",
		},
		{
			partition,
			where,
			orderByClause,
			"SELECT * FROM `test`.`t` PARTITION(`p0`) WHERE a > 10 ORDER BY a",
		},
		{
			"",
			where,
			orderByClause,
			"SELECT * FROM `test`.`t` WHERE a > 10 ORDER BY a",
		},
	}
	for _, testCase := range testCases {
		query := buildSelectQuery(dbName, tbName, fields, testCase.partition, testCase.where, testCase.orderByClause)
		c.Assert(query, Equals, testCase.expectedQuery)
	}
}

func (s *testSQLSuite) TestBuildRegionQueriesWithoutPartition(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	d := &Dumper{
		tctx:                      tctx,
		conf:                      DefaultConfig(),
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	d.conf.ServerInfo = ServerInfo{
		HasTiKV:       true,
		ServerType:    ServerTypeTiDB,
		ServerVersion: gcSafePointVersion,
	}
	database := "foo"
	table := "bar"

	testCases := []struct {
		regionResults        [][]driver.Value
		handleColNames       []string
		handleColTypes       []string
		expectedWhereClauses []string
		hasTiDBRowID         bool
	}{
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
			},
			[]string{"a"},
			[]string{"bigint"},
			[]string{
				"",
			},
			false,
		},
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
			},
			[]string{"_tidb_rowid"},
			[]string{"bigint"},
			[]string{
				"",
			},
			true,
		},
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
				{"7480000000000000FF335F728000000000FF0EA6010000000000FA", "tableID=51, _tidb_rowid=960001"},
				{"7480000000000000FF335F728000000000FF1D4C010000000000FA", "tableID=51, _tidb_rowid=1920001"},
				{"7480000000000000FF335F728000000000FF2BF2010000000000FA", "tableID=51, _tidb_rowid=2880001"},
			},
			[]string{"a"},
			[]string{"bigint"},
			[]string{
				"`a`<960001",
				"`a`>=960001 and `a`<1920001",
				"`a`>=1920001 and `a`<2880001",
				"`a`>=2880001",
			},
			false,
		},
		{
			[][]driver.Value{
				{"7480000000000000FF3300000000000000F8", "7480000000000000FF3300000000000000F8"},
				{"7480000000000000FF335F728000000000FF0EA6010000000000FA", "tableID=51, _tidb_rowid=960001"},
				// one invalid key
				{"7520000000000000FF335F728000000000FF0EA6010000000000FA", "7520000000000000FF335F728000000000FF0EA6010000000000FA"},
				{"7480000000000000FF335F728000000000FF1D4C010000000000FA", "tableID=51, _tidb_rowid=1920001"},
				{"7480000000000000FF335F728000000000FF2BF2010000000000FA", "tableID=51, _tidb_rowid=2880001"},
			},
			[]string{"_tidb_rowid"},
			[]string{"bigint"},
			[]string{
				"`_tidb_rowid`<960001",
				"`_tidb_rowid`>=960001 and `_tidb_rowid`<1920001",
				"`_tidb_rowid`>=1920001 and `_tidb_rowid`<2880001",
				"`_tidb_rowid`>=2880001",
			},
			true,
		},
	}

	for caseID, testCase := range testCases {
		c.Log(fmt.Sprintf("case #%d", caseID))
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes
		regionResults := testCase.regionResults

		// Test build tasks through table region
		taskChan := make(chan Task, 128)
		quotaCols := make([]string, 0, len(handleColNames))
		for _, col := range quotaCols {
			quotaCols = append(quotaCols, wrapBackTicks(col))
		}
		selectFields := strings.Join(quotaCols, ",")
		meta := &tableMeta{
			database:      database,
			table:         table,
			selectedField: selectFields,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}

		mock.ExpectQuery("SELECT PARTITION_NAME from INFORMATION_SCHEMA.PARTITIONS").
			WithArgs(database, table).WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).AddRow(nil))

		if testCase.hasTiDBRowID {
			mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
				WillReturnResult(sqlmock.NewResult(0, 0))
		} else {
			mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
				WillReturnError(&mysql.MyError{
					Code:    mysql.ER_BAD_FIELD_ERROR,
					State:   "42S22",
					Message: "Unknown column '_tidb_rowid' in 'field list'",
				})
			rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE"})
			for i := range handleColNames {
				rows.AddRow(handleColNames[i], handleColTypes[i])
			}
			mock.ExpectQuery("SELECT c.COLUMN_NAME, DATA_TYPE FROM").WithArgs(database, table).WillReturnRows(rows)
		}

		rows := sqlmock.NewRows([]string{"START_KEY", "tidb_decode_key(START_KEY)"})
		for _, regionResult := range regionResults {
			rows.AddRow(regionResult...)
		}
		mock.ExpectQuery("SELECT START_KEY,tidb_decode_key\\(START_KEY\\) from INFORMATION_SCHEMA.TIKV_REGION_STATUS").
			WithArgs(database, table).WillReturnRows(rows)

		rows = sqlmock.NewRows([]string{"COLUMN_NAME", "EXTRA"})
		for _, handleCol := range handleColNames {
			rows.AddRow(handleCol, "")
		}
		mock.ExpectQuery("SELECT COLUMN_NAME,EXTRA FROM INFORMATION_SCHEMA.COLUMNS").WithArgs(database, table).
			WillReturnRows(rows)

		orderByClause := buildOrderByClauseString(handleColNames)
		// special case, no enough value to split chunks
		if len(regionResults) <= 1 {
			mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
				WillReturnResult(sqlmock.NewResult(0, 0))
			orderByClause = orderByTiDBRowID
		}
		c.Assert(d.concurrentDumpTable(tctx, conn, meta, taskChan), IsNil)
		c.Assert(mock.ExpectationsWereMet(), IsNil)

		for i, w := range testCase.expectedWhereClauses {
			query := buildSelectQuery(database, table, "*", "", buildWhereCondition(d.conf, w), orderByClause)
			task := <-taskChan
			taskTableData, ok := task.(*TaskTableData)
			c.Assert(ok, IsTrue)
			c.Assert(taskTableData.ChunkIndex, Equals, i)
			data, ok := taskTableData.Data.(*tableData)
			c.Assert(ok, IsTrue)
			c.Assert(data.query, Equals, query)
		}
	}
}

func (s *testSQLSuite) TestBuildRegionQueriesWithPartitions(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	d := &Dumper{
		tctx:                      tctx,
		conf:                      DefaultConfig(),
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	d.conf.ServerInfo = ServerInfo{
		HasTiKV:       true,
		ServerType:    ServerTypeTiDB,
		ServerVersion: gcSafePointVersion,
	}
	database := "foo"
	table := "bar"
	partitions := []string{"p0", "p1", "p2"}

	testCases := []struct {
		regionResults        [][][]driver.Value
		handleColNames       []string
		handleColTypes       []string
		expectedWhereClauses [][]string
		hasTiDBRowID         bool
		dumpWholeTable       bool
	}{
		{
			[][][]driver.Value{
				{
					{6009, "t_121_i_1_0380000000000ea6010380000000000ea601", "t_121_", 6010, 1, 6010, 0, 0, 0, 74, 1052002},
					{6011, "t_121_", "t_121_i_1_0380000000000ea6010380000000000ea601", 6012, 1, 6012, 0, 0, 0, 68, 972177},
				},
				{
					{6015, "t_122_i_1_0380000000002d2a810380000000002d2a81", "t_122_", 6016, 1, 6016, 0, 0, 0, 77, 1092962},
					{6017, "t_122_", "t_122_i_1_0380000000002d2a810380000000002d2a81", 6018, 1, 6018, 0, 0, 0, 66, 939975},
				},
				{
					{6021, "t_123_i_1_0380000000004baf010380000000004baf01", "t_123_", 6022, 1, 6022, 0, 0, 0, 85, 1206726},
					{6023, "t_123_", "t_123_i_1_0380000000004baf010380000000004baf01", 6024, 1, 6024, 0, 0, 0, 65, 927576},
				},
			},
			[]string{"_tidb_rowid"},
			[]string{"bigint"},
			[][]string{
				{""}, {""}, {""},
			},
			true,
			true,
		},
		{
			[][][]driver.Value{
				{
					{6009, "t_121_i_1_0380000000000ea6010380000000000ea601", "t_121_r_10001", 6010, 1, 6010, 0, 0, 0, 74, 1052002},
					{6013, "t_121_r_10001", "t_121_r_970001", 6014, 1, 6014, 0, 0, 0, 75, 975908},
					{6003, "t_121_r_970001", "t_122_", 6004, 1, 6004, 0, 0, 0, 79, 1022285},
					{6011, "t_121_", "t_121_i_1_0380000000000ea6010380000000000ea601", 6012, 1, 6012, 0, 0, 0, 68, 972177},
				},
				{
					{6015, "t_122_i_1_0380000000002d2a810380000000002d2a81", "t_122_r_2070760", 6016, 1, 6016, 0, 0, 0, 77, 1092962},
					{6019, "t_122_r_2070760", "t_122_r_3047115", 6020, 1, 6020, 0, 0, 0, 75, 959650},
					{6005, "t_122_r_3047115", "t_123_", 6006, 1, 6006, 0, 0, 0, 77, 992339},
					{6017, "t_122_", "t_122_i_1_0380000000002d2a810380000000002d2a81", 6018, 1, 6018, 0, 0, 0, 66, 939975},
				},
				{
					{6021, "t_123_i_1_0380000000004baf010380000000004baf01", "t_123_r_4186953", 6022, 1, 6022, 0, 0, 0, 85, 1206726},
					{6025, "t_123_r_4186953", "t_123_r_5165682", 6026, 1, 6026, 0, 0, 0, 74, 951379},
					{6007, "t_123_r_5165682", "t_124_", 6008, 1, 6008, 0, 0, 0, 71, 918488},
					{6023, "t_123_", "t_123_i_1_0380000000004baf010380000000004baf01", 6024, 1, 6024, 0, 0, 0, 65, 927576},
				},
			},
			[]string{"_tidb_rowid"},
			[]string{"bigint"},
			[][]string{
				{
					"`_tidb_rowid`<10001",
					"`_tidb_rowid`>=10001 and `_tidb_rowid`<970001",
					"`_tidb_rowid`>=970001",
				},
				{
					"`_tidb_rowid`<2070760",
					"`_tidb_rowid`>=2070760 and `_tidb_rowid`<3047115",
					"`_tidb_rowid`>=3047115",
				},
				{
					"`_tidb_rowid`<4186953",
					"`_tidb_rowid`>=4186953 and `_tidb_rowid`<5165682",
					"`_tidb_rowid`>=5165682",
				},
			},
			true,
			false,
		},
		{
			[][][]driver.Value{
				{
					{6041, "t_134_", "t_134_r_960001", 6042, 1, 6042, 0, 0, 0, 69, 964987},
					{6035, "t_134_r_960001", "t_135_", 6036, 1, 6036, 0, 0, 0, 75, 1052130},
				},
				{
					{6043, "t_135_", "t_135_r_2960001", 6044, 1, 6044, 0, 0, 0, 69, 969576},
					{6037, "t_135_r_2960001", "t_136_", 6038, 1, 6038, 0, 0, 0, 72, 1014464},
				},
				{
					{6045, "t_136_", "t_136_r_4960001", 6046, 1, 6046, 0, 0, 0, 68, 957557},
					{6039, "t_136_r_4960001", "t_137_", 6040, 1, 6040, 0, 0, 0, 75, 1051579},
				},
			},
			[]string{"a"},
			[]string{"bigint"},
			[][]string{

				{
					"`a`<960001",
					"`a`>=960001",
				},
				{
					"`a`<2960001",
					"`a`>=2960001",
				},
				{
					"`a`<4960001",
					"`a`>=4960001",
				},
			},
			false,
			false,
		},
	}

	for i, testCase := range testCases {
		c.Log(fmt.Sprintf("case #%d", i))
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes
		regionResults := testCase.regionResults

		// Test build tasks through table region
		taskChan := make(chan Task, 128)
		quotaCols := make([]string, 0, len(handleColNames))
		for _, col := range quotaCols {
			quotaCols = append(quotaCols, wrapBackTicks(col))
		}
		selectFields := strings.Join(quotaCols, ",")
		meta := &tableMeta{
			database:      database,
			table:         table,
			selectedField: selectFields,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}

		rows := sqlmock.NewRows([]string{"PARTITION_NAME"})
		for _, partition := range partitions {
			rows.AddRow(partition)
		}
		mock.ExpectQuery("SELECT PARTITION_NAME from INFORMATION_SCHEMA.PARTITIONS").
			WithArgs(database, table).WillReturnRows(rows)

		if testCase.hasTiDBRowID {
			mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
				WillReturnResult(sqlmock.NewResult(0, 0))
		} else {
			mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
				WillReturnError(&mysql.MyError{
					Code:    mysql.ER_BAD_FIELD_ERROR,
					State:   "42S22",
					Message: "Unknown column '_tidb_rowid' in 'field list'",
				})
			rows = sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE"})
			for i := range handleColNames {
				rows.AddRow(handleColNames[i], handleColTypes[i])
			}
			mock.ExpectQuery("SELECT c.COLUMN_NAME, DATA_TYPE FROM").WithArgs(database, table).WillReturnRows(rows)
		}

		for i, partition := range partitions {
			rows = sqlmock.NewRows([]string{"REGION_ID", "START_KEY", "END_KEY", "LEADER_ID", "LEADER_STORE_ID", "PEERS", "SCATTERING", "WRITTEN_BYTES", "READ_BYTES", "APPROXIMATE_SIZE(MB)", "APPROXIMATE_KEYS"})
			for _, regionResult := range regionResults[i] {
				rows.AddRow(regionResult...)
			}
			mock.ExpectQuery(fmt.Sprintf("SHOW TABLE `%s`.`%s` PARTITION\\(`%s`\\) REGIONS", escapeString(database), escapeString(table), escapeString(partition))).
				WillReturnRows(rows)
		}

		for range partitions {
			rows = sqlmock.NewRows([]string{"COLUMN_NAME", "EXTRA"})
			for _, handleCol := range handleColNames {
				rows.AddRow(handleCol, "")
			}
			mock.ExpectQuery("SELECT COLUMN_NAME,EXTRA FROM INFORMATION_SCHEMA.COLUMNS").WithArgs(database, table).
				WillReturnRows(rows)
			// special case, dump whole table
			if testCase.dumpWholeTable {
				mock.ExpectExec(fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", database, table)).
					WillReturnResult(sqlmock.NewResult(0, 0))
			}
		}

		orderByClause := buildOrderByClauseString(handleColNames)
		c.Assert(d.concurrentDumpTable(tctx, conn, meta, taskChan), IsNil)
		c.Assert(mock.ExpectationsWereMet(), IsNil)

		chunkIdx := 0
		for i, partition := range partitions {
			for _, w := range testCase.expectedWhereClauses[i] {
				query := buildSelectQuery(database, table, "*", partition, buildWhereCondition(d.conf, w), orderByClause)
				task := <-taskChan
				taskTableData, ok := task.(*TaskTableData)
				c.Assert(ok, IsTrue)
				c.Assert(taskTableData.ChunkIndex, Equals, chunkIdx)
				data, ok := taskTableData.Data.(*tableData)
				c.Assert(ok, IsTrue)
				c.Assert(data.query, Equals, query)
				chunkIdx++
			}
		}
	}
}

func buildMockNewRows(mock sqlmock.Sqlmock, columns []string, driverValues [][]driver.Value) *sqlmock.Rows {
	rows := mock.NewRows(columns)
	for _, driverValue := range driverValues {
		rows.AddRow(driverValue...)
	}
	return rows
}

func readRegionCsvDriverValues(c *C) [][]driver.Value {
	content, err := os.ReadFile("./region_results.csv")
	c.Assert(err, IsNil)
	lines := strings.Split(string(content), "\n")
	values := make([][]driver.Value, 0, len(lines))
	for _, line := range lines {
		results := strings.Split(line, ",")
		if len(results) != 3 {
			continue
		}
		regionID, err := strconv.Atoi(results[0])
		c.Assert(err, IsNil)
		startKey := strings.Trim(results[1], `"`)
		endKey := strings.Trim(results[2], `"`)
		values = append(values, []driver.Value{regionID, startKey, endKey})
	}
	return values
}

func (s *testSQLSuite) TestBuildVersion3RegionQueries(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	conf := DefaultConfig()
	conf.ServerInfo = ServerInfo{
		HasTiKV:       true,
		ServerType:    ServerTypeTiDB,
		ServerVersion: decodeRegionVersion,
	}
	database := "test"
	conf.Tables = DatabaseTables{
		database: []*TableInfo{
			{"t1", TableTypeBase},
			{"t2", TableTypeBase},
			{"t3", TableTypeBase},
			{"t4", TableTypeBase},
		},
	}
	d := &Dumper{
		tctx:                      tctx,
		conf:                      conf,
		cancelCtx:                 cancel,
		selectTiDBTableRegionFunc: selectTiDBTableRegion,
	}
	showStatsHistograms := buildMockNewRows(mock, []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation"},
		[][]driver.Value{
			{"test", "t2", "p0", "a", 0, "2021-06-27 17:43:51", 1999999, 0, 8, 0},
			{"test", "t2", "p1", "a", 0, "2021-06-22 20:30:16", 1260000, 0, 8, 0},
			{"test", "t2", "p2", "a", 0, "2021-06-22 20:32:16", 1230000, 0, 8, 0},
			{"test", "t2", "p3", "a", 0, "2021-06-22 20:36:19", 2000000, 0, 8, 0},
			{"test", "t1", "", "a", 0, "2021-04-22 15:23:58", 7100000, 0, 8, 0},
			{"test", "t3", "", "PRIMARY", 1, "2021-06-27 22:08:43", 4980000, 0, 0, 0},
			{"test", "t4", "p0", "PRIMARY", 1, "2021-06-28 10:54:06", 2000000, 0, 0, 0},
			{"test", "t4", "p1", "PRIMARY", 1, "2021-06-28 10:55:04", 1300000, 0, 0, 0},
			{"test", "t4", "p2", "PRIMARY", 1, "2021-06-28 10:57:05", 1830000, 0, 0, 0},
			{"test", "t4", "p3", "PRIMARY", 1, "2021-06-28 10:59:04", 2000000, 0, 0, 0},
			{"mysql", "global_priv", "", "PRIMARY", 1, "2021-06-04 20:39:44", 0, 0, 0, 0},
		})
	selectMySQLStatsHistograms := buildMockNewRows(mock, []string{"TABLE_ID", "VERSION", "DISTINCT_COUNT"},
		[][]driver.Value{
			{15, 0, 0},
			{15, 0, 0},
			{15, 0, 0},
			{41, 424431121587240961, 7100000},
			{41, 424431121600348163, 7100000},
			{41, 424431121600348166, 7100000},
			{41, 424431121613455362, 7100000},
			{27, 0, 0},
			{27, 0, 0},
			{25, 0, 0},
			{25, 0, 0},
			{2098, 425410004549894148, 0},
			{2101, 425410005336064004, 0},
			{2101, 425410005336064004, 0},
			{2101, 425410005336064004, 0},
			{2101, 425410005336064004, 0},
			{2128, 425817527887331330, 1991680},
			{2128, 425817527912497153, 1991680},
			{2128, 425817527938973698, 1991680},
			{2129, 425817542815907841, 1260000},
			{2129, 425817542829539330, 1237120},
			{2129, 425817542842646529, 1237120},
			{2129, 425817542856278018, 1237120},
			{2130, 425817574444367873, 1230000},
			{2130, 425817574444367875, 1216128},
			{2130, 425817574483689474, 1216128},
			{2130, 425817574550274050, 1216128},
			{2131, 425817638001442817, 2000000},
			{2131, 425817638001442819, 1959424},
			{2131, 425817638027395074, 1959424},
			{2131, 425817638027395076, 1959424},
			{2128, 425928171654742033, 1999999},
			{2136, 425932336181018625, 4860000},
			{2136, 425932336194650114, 4860000},
			{2136, 425932336207757314, 4860000},
			{2136, 425932336220864514, 4860000},
			{2136, 425932337688608769, 4980000},
			{2139, 425944375634427905, 1991680},
			{2139, 425944375648059394, 1991680},
			{2139, 425944375648059396, 1991680},
			{2139, 425944375660904450, 1991680},
			{2139, 425944376067489793, 2000000},
			{2140, 425944390799720449, 1246336},
			{2140, 425944390799720451, 1246336},
			{2140, 425944390812303362, 1246336},
			{2140, 425944390838255618, 1246336},
			{2140, 425944391139983361, 1300000},
			{2141, 425944422492405761, 1780000},
			{2141, 425944422504988674, 1780000},
			{2141, 425944422504988676, 1780000},
			{2141, 425944422504988678, 1780000},
			{2141, 425944422898991105, 1830000},
			{2142, 425944453844828161, 1959424},
			{2142, 425944453857935362, 1959424},
			{2142, 425944453857935364, 1959424},
			{2142, 425944453869993986, 1959424},
			{2142, 425944454224150529, 2000000},
		})
	selectRegionStatusHistograms := buildMockNewRows(mock, []string{"REGION_ID", "START_KEY", "END_KEY"}, readRegionCsvDriverValues(c))
	selectInformationSchemaTables := buildMockNewRows(mock, []string{"TABLE_SCHEMA", "TABLE_NAME", "TIDB_TABLE_ID"},
		[][]driver.Value{
			{"mysql", "expr_pushdown_blacklist", 39},
			{"mysql", "user", 5},
			{"mysql", "db", 7},
			{"mysql", "tables_priv", 9},
			{"mysql", "stats_top_n", 37},
			{"mysql", "columns_priv", 11},
			{"mysql", "bind_info", 35},
			{"mysql", "default_roles", 33},
			{"mysql", "role_edges", 31},
			{"mysql", "stats_feedback", 29},
			{"mysql", "gc_delete_range_done", 27},
			{"mysql", "gc_delete_range", 25},
			{"mysql", "help_topic", 17},
			{"mysql", "global_priv", 2101},
			{"mysql", "stats_histograms", 21},
			{"mysql", "opt_rule_blacklist", 2098},
			{"mysql", "stats_meta", 19},
			{"mysql", "stats_buckets", 23},
			{"mysql", "tidb", 15},
			{"mysql", "GLOBAL_VARIABLES", 13},
			{"test", "t2", 2127},
			{"test", "t1", 41},
			{"test", "t3", 2136},
			{"test", "t4", 2138},
		})
	mock.ExpectQuery("SHOW STATS_HISTOGRAMS").
		WillReturnRows(showStatsHistograms)
	mock.ExpectQuery("SELECT TABLE_ID,VERSION,DISTINCT_COUNT FROM mysql.stats_histograms;").
		WillReturnRows(selectMySQLStatsHistograms)
	mock.ExpectQuery("SELECT TABLE_SCHEMA,TABLE_NAME,TIDB_TABLE_ID FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA").
		WillReturnRows(selectInformationSchemaTables)
	mock.ExpectQuery("SELECT REGION_ID,START_KEY,END_KEY FROM INFORMATION_SCHEMA.TIKV_REGION_STATUS ORDER BY START_KEY;").
		WillReturnRows(selectRegionStatusHistograms)

	c.Assert(d.renewSelectTableRegionFuncForLowerTiDB(tctx, conn), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	testCases := []struct {
		tableName            string
		handleColNames       []string
		handleColTypes       []string
		expectedWhereClauses []string
		hasTiDBRowID         bool
	}{
		{
			"t1",
			[]string{"a"},
			[]string{"int"},
			[]string{
				"`a`<960001",
				"`a`>=960001 and `a`<1920001",
				"`a`>=1920001 and `a`<2880001",
				"`a`>=2880001 and `a`<3840001",
				"`a`>=3840001 and `a`<4800001",
				"`a`>=4800001 and `a`<5760001",
				"`a`>=5760001 and `a`<6720001",
				"`a`>=6720001",
			},
			false,
		},
		{
			"t2",
			[]string{"a"},
			[]string{"int"},
			[]string{
				"`a`<960001",
				"`a`>=960001 and `a`<2960001",
				"`a`>=2960001 and `a`<4960001",
				"`a`>=4960001 and `a`<6960001",
				"`a`>=6960001",
			},
			false,
		},
		{
			"t3",
			[]string{"_tidb_rowid"},
			[]string{"int"},
			[]string{
				"`_tidb_rowid`<81584",
				"`_tidb_rowid`>=81584 and `_tidb_rowid`<1041584",
				"`_tidb_rowid`>=1041584 and `_tidb_rowid`<2001584",
				"`_tidb_rowid`>=2001584 and `_tidb_rowid`<2961584",
				"`_tidb_rowid`>=2961584 and `_tidb_rowid`<3921584",
				"`_tidb_rowid`>=3921584 and `_tidb_rowid`<4881584",
				"`_tidb_rowid`>=4881584 and `_tidb_rowid`<5841584",
				"`_tidb_rowid`>=5841584 and `_tidb_rowid`<6801584",
				"`_tidb_rowid`>=6801584",
			},
			true,
		},
		{
			"t4",
			[]string{"_tidb_rowid"},
			[]string{"int"},
			[]string{
				"`_tidb_rowid`<180001",
				"`_tidb_rowid`>=180001 and `_tidb_rowid`<1140001",
				"`_tidb_rowid`>=1140001 and `_tidb_rowid`<2200001",
				"`_tidb_rowid`>=2200001 and `_tidb_rowid`<3160001",
				"`_tidb_rowid`>=3160001 and `_tidb_rowid`<4160001",
				"`_tidb_rowid`>=4160001 and `_tidb_rowid`<5120001",
				"`_tidb_rowid`>=5120001 and `_tidb_rowid`<6170001",
				"`_tidb_rowid`>=6170001 and `_tidb_rowid`<7130001",
				"`_tidb_rowid`>=7130001",
			},
			true,
		},
	}

	for i, testCase := range testCases {
		c.Log(fmt.Sprintf("case #%d", i))
		table := testCase.tableName
		handleColNames := testCase.handleColNames
		handleColTypes := testCase.handleColTypes

		// Test build tasks through table region
		taskChan := make(chan Task, 128)
		quotaCols := make([]string, 0, len(handleColNames))
		for _, col := range quotaCols {
			quotaCols = append(quotaCols, wrapBackTicks(col))
		}
		selectFields := strings.Join(quotaCols, ",")
		meta := &tableMeta{
			database:      database,
			table:         table,
			selectedField: selectFields,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}

		if testCase.hasTiDBRowID {
			c.Log("mock succeed to select _tidb_rowid")
			mock.ExpectExec("SELECT _tidb_rowid").
				WillReturnResult(sqlmock.NewResult(0, 0))
		} else {
			c.Log("mock fail to select _tidb_rowid")
			mock.ExpectExec("SELECT _tidb_rowid").
				WillReturnError(&mysql.MyError{
					Code:    mysql.ER_BAD_FIELD_ERROR,
					State:   "42S22",
					Message: "Unknown column '_tidb_rowid' in 'field list'",
				})
			rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE"})
			for i := range handleColNames {
				rows.AddRow(handleColNames[i], handleColTypes[i])
			}
			mock.ExpectQuery("SELECT c.COLUMN_NAME, DATA_TYPE FROM").WithArgs(database, table).WillReturnRows(rows)
		}

		rows := sqlmock.NewRows([]string{"COLUMN_NAME", "EXTRA"})
		for _, handleCol := range handleColNames {
			rows.AddRow(handleCol, "")
		}
		mock.ExpectQuery("SELECT COLUMN_NAME,EXTRA FROM INFORMATION_SCHEMA.COLUMNS").WithArgs(database, table).
			WillReturnRows(rows)

		orderByClause := buildOrderByClauseString(handleColNames)
		err = d.concurrentDumpTable(tctx, conn, meta, taskChan)
		c.Log(err)
		c.Assert(err, IsNil)
		c.Assert(mock.ExpectationsWereMet(), IsNil)

		chunkIdx := 0
		for _, w := range testCase.expectedWhereClauses {
			query := buildSelectQuery(database, table, "*", "", buildWhereCondition(d.conf, w), orderByClause)
			task := <-taskChan
			taskTableData, ok := task.(*TaskTableData)
			c.Assert(ok, IsTrue)
			c.Assert(taskTableData.ChunkIndex, Equals, chunkIdx)
			data, ok := taskTableData.Data.(*tableData)
			c.Assert(ok, IsTrue)
			c.Assert(data.query, Equals, query)
			chunkIdx++
		}
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
