package dumpling

import (
	"database/sql"
	"github.com/pingcap/dumpling/v4/export"
)

// rowIter implements the SQLRowIter interface.
type rowIter struct {
	rows *sql.Rows
	args []interface{}
}

func (iter *rowIter) Next(row export.RowReceiver) error {
	err := decodeFromRows(iter.rows, iter.args, row)
	if err != nil {
		return err
	}
	return nil
}

func decodeFromRows(rows *sql.Rows, args []interface{}, row export.RowReceiver) error {
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

type stringIter struct {
	idx int
	ss  []string
}

func newStringIter(ss ...string) export.StringIter {
	return &stringIter{
		idx: 0,
		ss:  ss,
	}
}

func (m *stringIter) Next() string {
	if m.idx >= len(m.ss) {
		return ""
	}
	ret := m.ss[m.idx]
	m.idx += 1
	return ret
}

func (m *stringIter) HasNext() bool {
	return m.idx < len(m.ss)
}

type tableData struct {
	database string
	table    string
	rows     *sql.Rows
	colTypes []*sql.ColumnType
	specCmts []string
}

func (td *tableData) DatabaseName() string {
	return td.database
}

func (td *tableData) TableName() string {
	return td.table
}

func (td *tableData) ColumnCount() uint {
	return uint(len(td.colTypes))
}

func (td *tableData) Rows() export.SQLRowIter {
	return &rowIter{
		rows: td.rows,
		args: make([]interface{}, len(td.colTypes)),
	}
}

func (td *tableData) SpecialComments() export.StringIter {
	return newStringIter(td.specCmts...)
}

type metaData struct {
	target   string
	metaSQL  string
	specCmts []string
}

func (m *metaData) SpecialComments() export.StringIter {
	return newStringIter(m.specCmts...)
}

func (m *metaData) TargetName() string {
	return m.target
}

func (m *metaData) MetaSQL() string {
	return m.metaSQL
}
