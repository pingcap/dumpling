package export

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

const (
	clusterHandle = "clusterHandle="
	tidbRowID     = "_tidb_rowid="
	indexID       = "indexID="
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

func (iter *rowIter) Close() error {
	return iter.rows.Close()
}

func (iter *rowIter) Decode(row RowReceiver) error {
	return decodeFromRows(iter.rows, iter.args, row)
}

func (iter *rowIter) Error() error {
	return iter.rows.Err()
}

func (iter *rowIter) Next() {
	iter.hasNext = iter.rows.Next()
}

func (iter *rowIter) HasNext() bool {
	return iter.hasNext
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
	database        string
	table           string
	query           string
	chunkIndex      int
	rows            *sql.Rows
	colTypes        []*sql.ColumnType
	selectedField   string
	specCmts        []string
	escapeBackslash bool
	SQLRowIter
}

func (td *tableData) Start(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, td.query)
	if err != nil {
		return err
	}
	td.rows = rows
	return nil
}

func (td *tableData) ColumnTypes() []string {
	colTypes := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colTypes[i] = ct.DatabaseTypeName()
	}
	return colTypes
}

func (td *tableData) ColumnNames() []string {
	colNames := make([]string, len(td.colTypes))
	for i, ct := range td.colTypes {
		colNames[i] = ct.Name()
	}
	return colNames
}

func (td *tableData) DatabaseName() string {
	return td.database
}

func (td *tableData) TableName() string {
	return td.table
}

func (td *tableData) ChunkIndex() int {
	return td.chunkIndex
}

func (td *tableData) ColumnCount() uint {
	return uint(len(td.colTypes))
}

func (td *tableData) Rows() SQLRowIter {
	if td.SQLRowIter == nil {
		td.SQLRowIter = newRowIter(td.rows, len(td.colTypes))
	}
	return td.SQLRowIter
}

func (td *tableData) SelectedField() string {
	if td.selectedField == "*" {
		return ""
	}
	return fmt.Sprintf("(%s)", td.selectedField)
}

func (td *tableData) SpecialComments() StringIter {
	return newStringIter(td.specCmts...)
}

func (td *tableData) EscapeBackSlash() bool {
	return td.escapeBackslash
}

func splitTableDataIntoChunks(
	ctx context.Context,
	tableDataIRCh chan TableDataIR,
	errCh chan error,
	linear chan struct{},
	dbName, tableName string, db *sql.Conn, conf *Config) {
	if conf.ChunkByRegion {
		splitTableDataIntoChunksByRegion(
			ctx,
			tableDataIRCh,
			errCh,
			linear,
			dbName, tableName, db, conf)
		return
	}

	field, err := pickupPossibleField(dbName, tableName, db, conf)
	if err != nil {
		errCh <- withStack(err)
		return
	}
	if field == "" {
		// skip split chunk logic if not found proper field
		log.Debug("skip concurrent dump due to no proper field", zap.String("field", field))
		linear <- struct{}{}
		return
	}

	query := fmt.Sprintf("SELECT MIN(`%s`),MAX(`%s`) FROM `%s`.`%s` ",
		escapeString(field), escapeString(field), escapeString(dbName), escapeString(tableName))
	if conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, conf.Where)
	}
	log.Debug("split chunks", zap.String("query", query))

	var smin sql.NullString
	var smax sql.NullString
	row := db.QueryRowContext(ctx, query)
	err = row.Scan(&smin, &smax)
	if err != nil {
		log.Error("split chunks - get max min failed", zap.String("query", query), zap.Error(err))
		errCh <- withStack(err)
		return
	}
	if !smax.Valid || !smin.Valid {
		// found no data
		log.Warn("no data to dump", zap.String("schema", dbName), zap.String("table", tableName))
		close(tableDataIRCh)
		return
	}

	var max uint64
	var min uint64
	if max, err = strconv.ParseUint(smax.String, 10, 64); err != nil {
		errCh <- errors.WithMessagef(err, "fail to convert max value %s in query %s", smax.String, query)
		return
	}
	if min, err = strconv.ParseUint(smin.String, 10, 64); err != nil {
		errCh <- errors.WithMessagef(err, "fail to convert min value %s in query %s", smin.String, query)
		return
	}

	count := estimateCount(dbName, tableName, db, field, conf)
	log.Info("get estimated rows count", zap.Uint64("estimateCount", count))
	if count < conf.Rows {
		// skip chunk logic if estimates are low
		log.Debug("skip concurrent dump due to estimate count < rows",
			zap.Uint64("estimate count", count),
			zap.Uint64("conf.rows", conf.Rows),
		)
		linear <- struct{}{}
		return
	}

	// every chunk would have eventual adjustments
	estimatedChunks := count / conf.Rows
	estimatedStep := (max-min)/estimatedChunks + 1
	cutoff := min

	selectedField, err := buildSelectField(db, dbName, tableName, conf.CompleteInsert)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	colTypes, err := GetColumnTypes(db, selectedField, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}
	orderByClause, err := buildOrderByClause(conf, db, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	chunkIndex := 0
	nullValueCondition := fmt.Sprintf("`%s` IS NULL OR ", escapeString(field))
LOOP:
	for cutoff <= max {
		chunkIndex += 1
		where := fmt.Sprintf("%s(`%s` >= %d AND `%s` < %d)", nullValueCondition, escapeString(field), cutoff, escapeString(field), cutoff+estimatedStep)
		query = buildSelectQuery(dbName, tableName, selectedField, buildWhereCondition(conf, where), orderByClause)
		if len(nullValueCondition) > 0 {
			nullValueCondition = ""
		}

		td := &tableData{
			database:        dbName,
			table:           tableName,
			query:           query,
			chunkIndex:      chunkIndex,
			colTypes:        colTypes,
			selectedField:   selectedField,
			escapeBackslash: conf.EscapeBackslash,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}
		cutoff += estimatedStep
		select {
		case <-ctx.Done():
			break LOOP
		case tableDataIRCh <- td:
		}
	}
	close(tableDataIRCh)
}

func splitTableDataIntoChunksByRegion(
	ctx context.Context,
	tableDataIRCh chan TableDataIR,
	errCh chan error,
	linear chan struct{},
	dbName, tableName string, db *sql.Conn, conf *Config) {
	if conf.ServerInfo.ServerType != ServerTypeTiDB {
		errCh <- errors.Errorf("can't split chunks by region info for database %s except TiDB", serverTypeString[conf.ServerInfo.ServerType])
		return
	}

	startKeys, estimatedCounts, err := getTableRegionInfo(ctx, db, dbName, tableName)
	if err != nil {
		errCh <- errors.WithMessage(err, "fail to get TiDB table regions info")
		return
	}

	whereConditions, err := getWhereConditions(ctx, db, startKeys, estimatedCounts, conf.Rows, dbName, tableName)
	if err != nil {
		errCh <- errors.WithMessage(err, "fail to generate whereConditions")
		return
	}
	if len(whereConditions) <= 1 {
		linear <- struct{}{}
		return
	}

	selectedField, err := buildSelectField(db, dbName, tableName, conf.CompleteInsert)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	colTypes, err := GetColumnTypes(db, selectedField, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}
	orderByClause, err := buildOrderByClause(conf, db, dbName, tableName)
	if err != nil {
		errCh <- withStack(err)
		return
	}

	chunkIndex := 0
LOOP:
	for _, whereCondition := range whereConditions {
		chunkIndex += 1
		query := buildSelectQuery(dbName, tableName, selectedField, buildWhereCondition(conf, whereCondition), orderByClause)
		log.Debug("build region chunk query", zap.String("query", query), zap.Int("chunkIndex", chunkIndex),
			zap.String("db", dbName), zap.String("table", tableName))
		td := &tableData{
			database:        dbName,
			table:           tableName,
			query:           query,
			chunkIndex:      chunkIndex,
			colTypes:        colTypes,
			selectedField:   selectedField,
			escapeBackslash: conf.EscapeBackslash,
			specCmts: []string{
				"/*!40101 SET NAMES binary*/;",
			},
		}
		select {
		case <-ctx.Done():
			break LOOP
		case tableDataIRCh <- td:
		}
	}
	close(tableDataIRCh)
}

func tryDecodeRowKey(ctx context.Context, db *sql.Conn, key string) ([]string, error) {
	var (
		decodedRowKey string
		p             int
	)
	row := db.QueryRowContext(ctx, fmt.Sprintf("select tidb_decode_key('%s')", key))
	err := row.Scan(&decodedRowKey)
	if err != nil {
		return nil, err
	}
	if p = strings.Index(decodedRowKey, clusterHandle); p != -1 {
		p += len(clusterHandle)
		decodedRowKey = decodedRowKey[p:]
		if len(decodedRowKey) <= 2 {
			return nil, nil
		}
		keys := strings.Split(decodedRowKey[1:len(decodedRowKey)-1], ", ")
		return keys, nil
	} else if p = strings.Index(decodedRowKey, tidbRowID); p != -1 {
		p += len(tidbRowID)
		return []string{decodedRowKey[p:]}, nil
	} else if p = strings.Index(decodedRowKey, indexID); p != -1 {
		return nil, nil
	}
	return []string{"_tidb_rowid"}, nil
}

func getWhereConditions(ctx context.Context, db *sql.Conn, startKeys []string, counts []uint64, confRows uint64, dbName, tableName string) ([]string, error) {
	whereConditions := make([]string, 0)
	var (
		cutoff     uint64 = 0
		columnName []string
		dataType   []string
	)
	lastStartKey := ""
	rows, err := db.QueryContext(ctx, "SELECT s.COLUMN_NAME,t.DATA_TYPE FROM INFORMATION_SCHEMA.TIDB_INDEXES s, INFORMATION_SCHEMA.COLUMNS t WHERE s.KEY_NAME = 'PRIMARY' and s.TABLE_SCHEMA=t.TABLE_SCHEMA AND s.TABLE_NAME=t.TABLE_NAME AND s.COLUMN_NAME = t.COLUMN_NAME AND s.TABLE_SCHEMA=? AND s.TABLE_NAME=? ORDER BY s.SEQ_IN_INDEX;", dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var col, dType string
	for rows.Next() {
		err = rows.Scan(&col, &dType)
		if err != nil {
			return nil, err
		}
		columnName = append(columnName, fmt.Sprintf("`%s`", escapeString(col)))
		dataType = append(dataType, dType)
	}
	rows.Close()
	if len(columnName) == 0 {
		columnName = append(columnName, "_tidb_rowid")
		dataType = append(dataType, "int")
	}
	field := strings.Join(columnName, ",")

	generateWhereCondition := func(endKey string) {
		where := ""
		and := ""
		if lastStartKey != "" {
			where += fmt.Sprintf("(%s) >= (%s)", field, lastStartKey)
			and = " AND "
		}
		if endKey != "" {
			where += and
			where += fmt.Sprintf("(%s) < (%s)", field, endKey)
		}
		lastStartKey = endKey
		cutoff = 0
		whereConditions = append(whereConditions, where)
	}
	for i := 1; i < len(startKeys); i++ {
		keys, err := tryDecodeRowKey(ctx, db, startKeys[i])
		if err != nil {
			return nil, err
		}
		// omit indexID
		if len(dataType) == 0 {
			continue
		}
		cutoff += counts[i-1]
		if cutoff >= confRows {
			if len(dataType) != len(keys) {
				return nil, errors.Errorf("invalid column names %s and keys %s", columnName, keys)
			}
			var bf bytes.Buffer
			for j := range keys {
				if _, ok := dataTypeStringMap[strings.ToUpper(dataType[j])]; ok {
					bf.Reset()
					escape([]byte(keys[j]), &bf, nil)
					keys[j] = fmt.Sprintf("'%s'", bf.String())
				}
			}
			generateWhereCondition(strings.Join(keys, ","))
		}
	}
	cutoff += counts[len(startKeys)-1]
	if cutoff > 0 {
		generateWhereCondition("")
	}
	return whereConditions, nil
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
