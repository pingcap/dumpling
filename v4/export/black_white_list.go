package export

import (
	"strings"

	"github.com/pingcap/dumpling/v4/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"
)

type BWList interface {
	Apply(string, string) bool
}

type BWListMod byte

const (
	NopMode      BWListMod = 0x0
	OldToolsMode BWListMod = 0x1
)

type OldToolsConf struct {
	Rules         *filter.Rules
	CaseSensitive bool
}

type BWListConf struct {
	Mode BWListMod

	OldTools *OldToolsConf
}

type OldToolsBWList struct {
	*filter.Filter
}

func (bw *OldToolsBWList) Apply(schema, table string) bool {
	return bw.Match(&filter.Table{schema, table})
}

type NopeBWList struct{}

func (bw *NopeBWList) Apply(schema, table string) bool {
	return true
}

func NewBWList(conf BWListConf) (BWList, error) {
	switch conf.Mode {
	case OldToolsMode:
		oldToolsConf := conf.OldTools
		oldToolsBWList, err := filter.New(oldToolsConf.CaseSensitive, oldToolsConf.Rules)
		if err != nil {
			return nil, withStack(err)
		}

		return &OldToolsBWList{
			Filter: oldToolsBWList,
		}, nil
	}

	return &NopeBWList{}, nil
}

func filterDirtySchemaTables(conf *Config) {
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		for dbName := range conf.Tables {
			if filter.IsSystemSchema(strings.ToLower(dbName)) {
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
	ignoredDBTable := DatabaseTables{}
	bwList, err := NewBWList(conf.BlackWhiteList)
	if err != nil {
		return withStack(err)
	}

	for dbName, tables := range conf.Tables {
		for _, table := range tables {
			if bwList.Apply(dbName, table.Name) {
				dbTables.AppendTable(dbName, table)
			} else {
				ignoredDBTable.AppendTable(dbName, table)
			}
		}
	}

	if len(ignoredDBTable) > 0 {
		log.Zap().Debug("ignore table", zap.String("", ignoredDBTable.Literal()))
	}

	conf.Tables = dbTables
	return nil
}
