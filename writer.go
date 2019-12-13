package dumpling

import (
	"fmt"
)

type Writer interface {
	WriteDatabaseMeta(dbName string) error
	WriteTableMeta(db, table, createSQL string) error
	NewTableDumper(db, table string) tableDumper
}

type dummyWriter struct{}

func (dummyWriter) WriteDatabaseMeta(dbs string) error {
	fmt.Println("write databases", dbs)
	return nil
}

func (dummyWriter) WriteTableMeta(db, table, createSQL string) error {
	fmt.Println("write table meta", table, createSQL)
	return nil
}

func (dummyWriter) NewTableDumper(db, table string) tableDumper {
	return &dummyTableDumper{
		database: db,
		table:    table,
	}
}
