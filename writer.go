package dumpling

import (
	"context"
	"database/sql"
	"fmt"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, dbName string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	NewTableDumper(ctx context.Context, db, table string) tableDumper
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

func (dummyWriter) NewTableDumper(ctx context.Context, db, table string) tableDumper {
	return &dummyTableDumper{
		database: db,
		table:    table,
	}
}

type dummyTableDumper struct {
	database string
	table    string

	args []interface{}
	rows []stringSlice
}

type stringSlice []string

func (s stringSlice) prepareScanArgs(dst []interface{}) {
	for i := 0; i < len(s); i++ {
		dst[i] = &s[i]
	}
}

func (s *dummyTableDumper) prepareColumns(ctx context.Context, colTypes []*sql.ColumnType) {
	s.args = make([]interface{}, len(colTypes))
}

func (s *dummyTableDumper) handleOneRow(ctx context.Context, rows *sql.Rows) error {
	row := make(stringSlice, len(s.args))
	if err := decodeFromRows(rows, s.args, row); err != nil {
		rows.Close()
		return err
	}
	s.rows = append(s.rows, row)

	if len(s.rows) > 20 {
		s.flush()
	}
	return nil
}

func (s *dummyTableDumper) finishTable(ctx context.Context) {
	s.flush()
}

func (s *dummyTableDumper) flush() {
	for _, row := range s.rows {
		fmt.Printf("INSERT INTO %s.%s VALUES (", s.database, s.table)
		for i, col := range row {
			if i != 0 {
				fmt.Printf(", ")
			}
			fmt.Printf(col)
		}
		fmt.Println(")")
	}
	s.rows = s.rows[:0]
}
