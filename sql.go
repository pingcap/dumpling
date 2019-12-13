package dumpling

import (
	"database/sql"
	"fmt"
)

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

func (s *dummyTableDumper) prepareColumns(colTypes []*sql.ColumnType) {
	s.args = make([]interface{}, len(colTypes))
}

func (s *dummyTableDumper) handleOneRow(rows *sql.Rows) error {
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

func (s *dummyTableDumper) finishTable() {
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
