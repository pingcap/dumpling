// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"errors"
	"fmt"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/br/pkg/storage"
	. "github.com/pingcap/check"
)

var _ = Suite(&testMetaDataSuite{})

const (
	logFile = "ON.000001"
	pos     = "7502"
	gtidSet = "6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29"
)

type testMetaDataSuite struct{}

func (s *testMetaDataSuite) TestMysqlMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}))

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMySQL, false), IsNil)

	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: ON.000001\n"+
		"\tPos: 7502\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) createStorage(c *C) storage.ExternalStorage {
	backend, err := storage.ParseBackend("file:///"+c.MkDir(), nil)
	c.Assert(err, IsNil)
	testLoc, _ := storage.Create(context.Background(), backend, true)
	return testLoc
}

func (s *testMetaDataSuite) TestMetaDataAfterConn(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	pos2 := "7510"
	rows2 := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos2, "", "", gtidSet)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}))
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows2)

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMySQL, false), IsNil)
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMySQL, true), IsNil)
	m.buffer.Write(m.afterConnBuffer.Bytes())

	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: ON.000001\n"+
		"\tPos: 7502\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"+
		"SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */\n"+
		"\tLog: ON.000001\n"+
		"\tPos: 7510\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestMysqlWithFollowersMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	followerRows := sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}).
		AddRow("256529431", "mysql-bin.001821", "192.168.1.100", gtidSet, 0)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(followerRows)

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMySQL, false), IsNil)

	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: ON.000001\n"+
		"\tPos: 7502\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"+
		"SHOW SLAVE STATUS:\n"+
		"\tHost: 192.168.1.100\n"+
		"\tLog: mysql-bin.001821\n"+
		"\tPos: 256529431\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestMysqlWithNullFollowersMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(sqlmock.NewRows([]string{"SQL_Remaining_Delay"}).AddRow(nil))

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMySQL, false), IsNil)

	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: ON.000001\n"+
		"\tPos: 7502\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestMariaDBMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	logFile := "mariadb-bin.000016"
	pos := "475"
	gtidSet := "0-1-2"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"@@global.gtid_binlog_pos"}).
		AddRow(gtidSet)
	mock.ExpectQuery("SELECT @@global.gtid_binlog_pos").WillReturnRows(rows)
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(rows)
	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMariaDB, false), IsNil)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestMariaDBWithFollowersMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	followerRows := sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "connection_name", "Seconds_Behind_Master"}).
		AddRow("256529431", "mysql-bin.001821", "192.168.1.100", gtidSet, "connection_1", 0).
		AddRow("256529451", "mysql-bin.001820", "192.168.1.102", gtidSet, "connection_2", 200)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").
		WillReturnRows(sqlmock.NewRows([]string{"@@default_master_connection"}).
			AddRow("connection_1"))
	mock.ExpectQuery("SHOW ALL SLAVES STATUS").WillReturnRows(followerRows)

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMySQL, false), IsNil)

	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: ON.000001\n"+
		"\tPos: 7502\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"+
		"SHOW SLAVE STATUS:\n"+
		"\tConnection name: connection_1\n"+
		"\tHost: 192.168.1.100\n"+
		"\tLog: mysql-bin.001821\n"+
		"\tPos: 256529431\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"+
		"SHOW SLAVE STATUS:\n"+
		"\tConnection name: connection_2\n"+
		"\tHost: 192.168.1.102\n"+
		"\tLog: mysql-bin.001820\n"+
		"\tPos: 256529451\n"+
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestEarlierMysqlMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	logFile := "mysql-bin.000001"
	pos := "4879"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}))

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeMySQL, false), IsNil)

	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: mysql-bin.000001\n"+
		"\tPos: 4879\n"+
		"\tGTID:\n\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestTiDBSnapshotMetaData(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	logFile := "tidb-binlog"
	pos := "420633329401856001"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeTiDB, false), IsNil)
	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: tidb-binlog\n"+
		"\tPos: 420633329401856001\n"+
		"\tGTID:\n\n")

	snapshot := "420633273211289601"
	rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	m = newGlobalMetadata(tcontext.Background(), s.createStorage(c), snapshot)
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeTiDB, false), IsNil)
	c.Assert(m.buffer.String(), Equals, "SHOW MASTER STATUS:\n"+
		"\tLog: tidb-binlog\n"+
		"\tPos: 420633273211289601\n"+
		"\tGTID:\n\n")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testMetaDataSuite) TestNoPrivilege(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnError(errors.New("lack SUPER or REPLICATION CLIENT privilege"))

	m := newGlobalMetadata(tcontext.Background(), s.createStorage(c), "")
	// some consistencyType will ignore this error, this test make sure no extra message is written
	c.Assert(m.recordGlobalMetaData(conn, ServerTypeTiDB, false), NotNil)
	c.Assert(m.buffer.String(), Equals, "")
}
