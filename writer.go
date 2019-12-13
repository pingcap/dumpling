package dumpling

import (
	"context"
	"database/sql"
	"fmt"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, dbName string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteTableData(ctx context.Context, ir TableDataIR) error
}

type dummyWriter struct{}

func (dummyWriter) WriteDatabaseMeta(ctx context.Context, dbs string) error {
	fmt.Println("write databases", dbs)
	return nil
}

func (dummyWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fmt.Println("write table meta", table, createSQL)
	return nil
}

func (dummyWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	rowIter := ir.Rows()
	for rowIter.HasNext() {
		row := make(dummyRow, ir.ColumnNumber())
		err := rowIter.Next(row)
		if err != nil {
			return err
		}

		row.Print(ir.TableName())
	}
	return nil
}

type dummyRow []sql.NullString

func (s dummyRow) BindAddress(dst []interface{}) {
	for i := 0; i < len(s); i++ {
		dst[i] = &s[i]
	}
}

func (s dummyRow) Print(table string) {
	fmt.Printf("INSERT INTO %s VALUES (", table)
	for i, col := range s {
		if i != 0 {
			fmt.Printf(", ")
		}
		if col.Valid {
			fmt.Printf(col.String)
		} else {
			fmt.Printf("NULL")
		}
	}
	fmt.Println(")")
}
