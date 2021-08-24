// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"strings"

	tcontext "github.com/pingcap/dumpling/v4/context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tf "github.com/pingcap/tidb-tools/pkg/table-filter"
)

var _ = Suite(&testBWListSuite{})

type testBWListSuite struct{}

func (s *testBWListSuite) TestFilterTables(c *C) {
	tctx := tcontext.Background().WithLogger(appLogger)
	dbTables := DatabaseTables{}
	expectedDBTables := DatabaseTables{}

	dbTables.AppendTables(filter.InformationSchemaName, []string{"xxx"}, []uint64{0})
	dbTables.AppendTables(strings.ToUpper(filter.PerformanceSchemaName), []string{"xxx"}, []uint64{0})
	dbTables.AppendTables("xxx", []string{"yyy"}, []uint64{0})
	expectedDBTables.AppendTables("xxx", []string{"yyy"}, []uint64{0})
	dbTables.AppendTables("yyy", []string{"xxx"}, []uint64{0})

	tableFilter, err := tf.Parse([]string{"*.*"})
	c.Assert(err, IsNil)
	conf := &Config{
		ServerInfo: ServerInfo{
			ServerType: ServerTypeTiDB,
		},
		Tables:      dbTables,
		TableFilter: tableFilter,
	}

	databases := []string{filter.InformationSchemaName, filter.PerformanceSchemaName, "xxx", "yyy"}
	c.Assert(filterDataBases(tctx, conf, databases), DeepEquals, databases)

	conf.TableFilter = tf.NewSchemasFilter("xxx")
	c.Assert(filterDataBases(tctx, conf, databases), DeepEquals, []string{"xxx"})
	filterTables(tcontext.Background(), conf)
	c.Assert(conf.Tables, HasLen, 1)
	c.Assert(conf.Tables, DeepEquals, expectedDBTables)
}

func (s *testBWListSuite) TestFilterDatabaseWithNoTable(c *C) {
	dbTables := DatabaseTables{}
	expectedDBTables := DatabaseTables{}

	dbTables["xxx"] = []*TableInfo{}
	conf := &Config{
		ServerInfo: ServerInfo{
			ServerType: ServerTypeTiDB,
		},
		Tables:            dbTables,
		TableFilter:       tf.NewSchemasFilter("yyy"),
		DumpEmptyDatabase: true,
	}
	filterTables(tcontext.Background(), conf)
	c.Assert(conf.Tables, HasLen, 0)

	dbTables["xxx"] = []*TableInfo{}
	expectedDBTables["xxx"] = []*TableInfo{}
	conf.Tables = dbTables
	conf.TableFilter = tf.NewSchemasFilter("xxx")
	filterTables(tcontext.Background(), conf)
	c.Assert(conf.Tables, HasLen, 1)
	c.Assert(conf.Tables, DeepEquals, expectedDBTables)

	dbTables["xxx"] = []*TableInfo{}
	expectedDBTables = DatabaseTables{}
	conf.Tables = dbTables
	conf.DumpEmptyDatabase = false
	filterTables(tcontext.Background(), conf)
	c.Assert(conf.Tables, HasLen, 0)
	c.Assert(conf.Tables, DeepEquals, expectedDBTables)
}
