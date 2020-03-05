package export

import (
	"database/sql"
	"strings"

	"github.com/pingcap/dumpling/v4/log"
	"go.uber.org/zap"
	"github.com/pingcap/tidb/util"
)

func detectServerInfo(db *sql.DB) (ServerInfo, error) {
	versionStr, err := SelectVersion(db)
	if err != nil {
		return ServerInfoUnknown, err
	}
	return ParseServerInfo(versionStr), nil
}

func prepareDumpingDatabases(conf *Config, db *sql.DB) ([]string, error) {
	if conf.Database == "" {
		return ShowDatabases(db)
	} else {
		return strings.Split(conf.Database, ","), nil
	}
}

func listAllTables(db *sql.DB, databaseNames []string) (DatabaseTables, error) {
	log.Zap().Debug("list all the tables")
	dbTables := DatabaseTables{}
	for _, dbName := range databaseNames {
		tables, err := ListAllTables(db, dbName)
		if err != nil {
			return nil, err
		}
		dbTables = dbTables.AppendTables(dbName, tables...)
	}
	return dbTables, nil
}

func listAllViews(db *sql.DB, databaseNames []string) (DatabaseTables, error) {
	log.Zap().Debug("list all the views")
	dbTables := DatabaseTables{}
	for _, dbName := range databaseNames {
		views, err := ListAllViews(db, dbName)
		if err != nil {
			return nil, err
		}
		dbTables = dbTables.AppendViews(dbName, views...)
	}
	return dbTables, nil
}

type databaseName = string

type TableType int8

const (
	TableTypeBase TableType = iota
	TableTypeView
)

type TableInfo struct {
	Name string
	Type TableType
}

func (t *TableInfo) Equals(other *TableInfo) bool {
	return t.Name == other.Name && t.Type == other.Type
}

type DatabaseTables map[databaseName][]*TableInfo

func NewDatabaseTables() DatabaseTables {
	return DatabaseTables{}
}

func (d DatabaseTables) AppendTables(dbName string, tableNames ...string) DatabaseTables {
	for _, t := range tableNames {
		d[dbName] = append(d[dbName], &TableInfo{t, TableTypeBase})
	}
	return d
}

func (d DatabaseTables) AppendViews(dbName string, viewNames ...string) DatabaseTables {
	for _, v := range viewNames {
		d[dbName] = append(d[dbName], &TableInfo{v, TableTypeView})
	}
	return d
}

func (d DatabaseTables) Merge(other DatabaseTables) {
	for name, infos := range other {
		d[name] = append(d[name], infos...)
	}
}

func filterDirtySchemaTables(conf *Config) {
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		for dbName := range conf.Tables {
			switch strings.ToLower(dbName) {
			case util.InformationSchemaName.L, util.PerformanceSchemaName.L, util.MetricSchemaName.L, util.InspectionSchemaName.L:
				log.Zap().Warn("unsupported dump schema in TiDB now", zap.String("schema", dbName))
				delete(conf.Tables, dbName)
			}
		}
	}
}

func filterTables(conf *Config) error {
	log.Zap().Debug("filter tables")
	// filter dirty schema tables because of non-impedance implementation reasons
	filterDirtySchemaTables(conf)
	dbTables := DatabaseTables{}
	bwList, err := NewBWList(conf.BlackWhiteList)
	if err != nil {
		return withStack(err)
	}

	for dbName, tables := range conf.Tables {
		doTables := make([]string, 0, len(tables))
		for _, table := range tables {
			if bwList.Apply(dbName, table.Name) {
				doTables = append(doTables, table.Name)
			}
		}
		if len(doTables) > 0 {
			dbTables = dbTables.AppendTables(dbName, doTables...)
		}
	}

	conf.Tables = dbTables
	return nil
}
