package export

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/pkg/errors"
)

// rowIter implements the SQLRowIter interface.
// Note: To create a rowIter, please use `newRowIter()` instead of struct literal.
type rowIter struct {
	rows    *sql.Rows
	hasNext bool
	args    []interface{}
}

func newRowIter(rows *sql.Rows, argLen int) *rowIter {
	r := &rowIter{
		rows:    rows,
		hasNext: false,
		args:    make([]interface{}, argLen),
	}
	r.hasNext = r.rows.Next()
	return r
}

func (iter *rowIter) Next(row RowReceiver) error {
	err := decodeFromRows(iter.rows, iter.args, row)
	iter.hasNext = iter.rows.Next()
	return err
}

func (iter *rowIter) HasNext() bool {
	return iter.hasNext
}

func (iter *rowIter) HasNextSQLRowIter() bool {
	return iter.hasNext
}

func (iter *rowIter) NextSQLRowIter() SQLRowIter {
	return iter
}

type fileRowIter struct {
	rowIter            SQLRowIter
	fileSizeLimit      uint64
	statementSizeLimit uint64

	currentStatementSize uint64
	currentFileSize      uint64
}

func (c *fileRowIter) Next(row RowReceiver) error {
	err := c.rowIter.Next(row)
	if err != nil {
		return err
	}

	size := row.ReportSize()
	c.currentFileSize += size
	c.currentStatementSize += size
	return nil
}

func (c *fileRowIter) HasNext() bool {
	if c.fileSizeLimit != UnspecifiedSize && c.currentFileSize >= c.fileSizeLimit {
		return false
	}

	if c.statementSizeLimit != UnspecifiedSize && c.currentStatementSize >= c.statementSizeLimit {
		return false
	}
	return c.rowIter.HasNext()
}

func (c *fileRowIter) HasNextSQLRowIter() bool {
	if c.fileSizeLimit != UnspecifiedSize && c.currentFileSize >= c.fileSizeLimit {
		return false
	}
	return c.rowIter.HasNext()
}

func (c *fileRowIter) NextSQLRowIter() SQLRowIter {
	return &fileRowIter{
		rowIter:              c.rowIter,
		fileSizeLimit:        c.fileSizeLimit,
		statementSizeLimit:   c.statementSizeLimit,
		currentFileSize:      c.currentFileSize,
		currentStatementSize: 0,
	}
}

type stringIter struct {
	idx int
	ss  []string
}

func newStringIter(ss ...string) StringIter {
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

func (td *tableData) ColumnTypes() []string {
	colTypes := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colTypes[i] = ct.DatabaseTypeName()
	}
	return colTypes
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

func (td *tableData) Rows() SQLRowIter {
	return newRowIter(td.rows, len(td.colTypes))
}

func (td *tableData) SpecialComments() StringIter {
	return newStringIter(td.specCmts...)
}

type tableDataChunks struct {
	TableDataIR
	rows               SQLRowIter
	chunkSizeLimit     uint64
	statementSizeLimit uint64
}

func (t *tableDataChunks) Rows() SQLRowIter {
	if t.rows == nil {
		t.rows = t.TableDataIR.Rows()
	}

	return &fileRowIter{
		rowIter:            t.rows,
		statementSizeLimit: t.statementSizeLimit,
		fileSizeLimit:      t.chunkSizeLimit,
	}
}

func splitTableDataIntoChunksIter(td TableDataIR, chunkSize uint64, statementSize uint64) *tableDataChunks {
	return &tableDataChunks{
		TableDataIR:        td,
		chunkSizeLimit:     chunkSize,
		statementSizeLimit: statementSize,
	}
}


func splitTableDataIntoChunks(dbName, tableName string, db *sql.DB, conf *Config) ([]*tableDataChunks, error) {
	field, err := pickupPossibleField(dbName, tableName, db, conf)
	if err != nil {
		return nil, err
	}
	if field == "" {
		return nil, nil
	}
	query := fmt.Sprintf("SELECT MIN(`%s`),MAX(`%s`) FROM `%s`.`%s` ",
		field, field, dbName, tableName)
	if conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, conf.Where)
	}

	var smin string
	var smax string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&smin, &smax)
	}
	var max uint64
	var min uint64
	if max, err = strconv.ParseUint(smax, 10, 64); err != nil {
		return nil, nil
	}
	if min, err = strconv.ParseUint(smin, 10, 64); err != nil {
		return nil, nil
	}
	err = simpleQuery(db, query, handleOneRow)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}
	if min == max {
		return nil, nil
	}
	count := estimateCount(dbName, tableName, db, field, conf)
	if count < conf.Rows {
		// skip chunk logic if estimates are low
		return nil, nil
	}
	// every chunk would have eventual adjustments
	estimatedChunks := count / conf.Rows
	estimatedStep := (max-min)/estimatedChunks + 1
	cutoff := min
	chunks := make([]tableDataChunks, 0, estimatedChunks)

	colTypes, err := GetColumnTypes(db, dbName, tableName)
	if err != nil {
		return nil, err
	}
	for cutoff <= max {
		where := fmt.Sprintf("(`%s` >= %d AND `%s` < %d)", field, cutoff, field, cutoff+estimatedStep)
		query, err = buildSelectAllQuery(conf, db, dbName, tableName, where)
		if err != nil {
			return nil, err
		}
		rows, err := db.Query(query)
		if err != nil {
			return nil, withStack(errors.WithMessage(err, query))
		}

		td := &tableData{
			database: dbName,
			table:    tableName,
			rows:     rows,
			colTypes: colTypes,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}
		cutoff += estimatedStep
		chunk := tableDataChunks{
			TableDataIR: td,
		}
		chunks = append(chunks, chunk)
	}

	return nil, nil
}

type metaData struct {
	target   string
	metaSQL  string
	specCmts []string
}

func (m *metaData) SpecialComments() StringIter {
	return newStringIter(m.specCmts...)
}

func (m *metaData) TargetName() string {
	return m.target
}

func (m *metaData) MetaSQL() string {
	return m.metaSQL
}
