// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"github.com/pingcap/dumpling/v4/log"
	"go.uber.org/zap"
)

func filterTables(conf *Config) {
	log.Debug("filter tables")
	dbTables := DatabaseTables{}
	ignoredDBTable := DatabaseTables{}

	for dbName, tables := range conf.Tables {
		for _, table := range tables {
			if conf.TableFilter.MatchTable(dbName, table.Name) {
				dbTables.AppendTable(dbName, table)
			} else {
				ignoredDBTable.AppendTable(dbName, table)
			}
		}
		// 1. this dbName doesn't match block allow list, don't add
		// 2. this dbName matches block allow list, but there is no table in this database, add
		if conf.DumpEmptyDatabase {
			if _, ok := dbTables[dbName]; !ok && conf.TableFilter.MatchSchema(dbName) {
				dbTables[dbName] = make([]*TableInfo, 0)
			}
		}
	}

	if len(ignoredDBTable) > 0 {
		log.Debug("ignore table", zap.String("", ignoredDBTable.Literal()))
	}

	conf.Tables = dbTables
}
