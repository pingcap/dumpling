// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"errors"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
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

	mockConf := DefaultConfig()
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
	q := buildSelectQuery("test", "t", selectedField, "", orderByClause)
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
	q = buildSelectQuery("test", "t", selectedField, "", orderByClause)
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
		q = buildSelectQuery("test", "t", selectedField, "", orderByClause)
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
		q := buildSelectQuery("test", "t", selectedField, "", orderByClause)
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
		q := buildSelectQuery("test", "t", selectedField, "", "")
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

	mockConf := DefaultConfig()
	mockConf.SortByPk = true

	// Test TiDB server.
	mockConf.ServerInfo.ServerType = ServerTypeTiDB

	// _tidb_rowid is available.
	mock.ExpectExec("SELECT _tidb_rowid from `test`.`t`").
		WillReturnResult(sqlmock.NewResult(0, 0))

	orderByClause, err := buildOrderByClause(mockConf, conn, "test", "t")
	c.Assert(err, IsNil)
	c.Assert(orderByClause, Equals, "ORDER BY `_tidb_rowid`")

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

func makeVersion(major, minor, patch int64, preRelease string) *semver.Version {
	return &semver.Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		PreRelease: semver.PreRelease(preRelease),
		Metadata:   "",
	}
}
