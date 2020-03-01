package export

import (
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testPrepareSuite{})

type testPrepareSuite struct{}

func (s *testPrepareSuite) TestPrepareDumpingDatabases(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	conf := DefaultConfig()
	conf.Database = "db1,db2,db3"
	result, err := prepareDumpingDatabases(conf, db)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2", "db3"})

	conf.Database = ""
	rows := sqlmock.NewRows([]string{"Database"}).
		AddRow("db1").
		AddRow("db2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	result, err = prepareDumpingDatabases(conf, db)
	c.Assert(err, IsNil)
	c.Assert(result, DeepEquals, []string{"db1", "db2"})

	mock.ExpectQuery("SHOW DATABASES").WillReturnError(fmt.Errorf("err"))
	_, err = prepareDumpingDatabases(conf, db)
	c.Assert(err, NotNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *testPrepareSuite) TestListAllTables(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	data := map[databaseName][]*TableInfo{
		"db1": NewTableInfos([]string{"t1", "t2"}, TableTypeBase),
		"db2": NewTableInfos([]string{"t3", "t4", "t5"}, TableTypeBase),
		"db3": NewTableInfos([]string{"t6", "t7", "t8"}, TableTypeView),
	}

	var dbNames []databaseName
	for dbName, tableInfos := range data {
		dbNames = append(dbNames, dbName)

		rows := sqlmock.NewRows([]string{"Table_name"})
		for _, tbInfo := range tableInfos {
			if tbInfo.Type == TableTypeView {
				continue
			}
			rows.AddRow(tbInfo.Name)
		}
		query := "SELECT table_name FROM information_schema.tables WHERE table_schema = (.*) and table_type = (.*)"
		mock.ExpectQuery(query).WillReturnRows(rows)
	}

	tables, err := listAllTables(db, dbNames, true)
	c.Assert(err, IsNil)

	for d, t := range tables {
		expectedTbs, ok := data[d]
		c.Assert(ok, IsTrue)
		for i := 0; i < len(t); i++ {
			cmt := Commentf("%v mismatch: %v", t[i], expectedTbs[i])
			c.Assert(t[i].Equals(expectedTbs[i]), IsTrue, cmt)
		}
	}

	// Test list all tables and not skipping views.
	data = map[databaseName][]*TableInfo{}
	data["db"] = append(data["db"], &TableInfo{"t1", TableTypeBase})
	data["db"] = append(data["db"], &TableInfo{"t2", TableTypeView})
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = (.*) and table_type = (.*)"
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"Table_name"}).AddRow("t1"))
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"Table_name"}).AddRow("t2"))
	tables, err = listAllTables(db, []string{"db"}, false)
	c.Assert(err, IsNil)
	c.Assert(len(tables), Equals, 1)
	c.Assert(len(tables["db"]), Equals, 2)
	c.Assert(tables["db"][0].Equals(data["db"][0]), IsTrue)
	c.Assert(tables["db"][1].Equals(data["db"][1]), IsTrue)

	c.Assert(mock.ExpectationsWereMet(), IsNil)
}
