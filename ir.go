package dumpling

import (
	"database/sql"
)

type TableDataIR interface {
	TableName() string
	ColumnNumber() int
	Rows() SQLRowIter
}

type SQLRowIter interface {
	Next(to RowReceiver) error
	HasNext() bool
}

type RowReceiver interface {
	BindAddress([]interface{})
}

// rowIter implements the SQLRowIter interface.
type rowIter struct {
	rows *sql.Rows
	args []interface{}
}

func (iter *rowIter) Next(row RowReceiver) error {
	err := decodeFromRows(iter.rows, iter.args, row)
	if err != nil {
		return err
	}
	return nil
}

func decodeFromRows(rows *sql.Rows, args []interface{}, row RowReceiver) error {
	row.BindAddress(args)
	if err := rows.Scan(args...); err != nil {
		rows.Close()
		return withStack(err)
	}
	return nil
}

func (iter *rowIter) HasNext() bool {
	return iter.rows.Next()
}

type tableData struct {
	database string
	table    string
	rows     *sql.Rows
	colTypes []*sql.ColumnType
}

func (td *tableData) TableName() string {
	return td.table
}

func (td *tableData) ColumnNumber() int {
	return len(td.colTypes)
}

func (td *tableData) Rows() SQLRowIter {
	return &rowIter{
		rows: td.rows,
		args: make([]interface{}, len(td.colTypes)),
	}
}
