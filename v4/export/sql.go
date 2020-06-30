package export

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

func ShowDatabases(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW DATABASES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

// ShowTables shows the tables of a database, the caller should use the correct database.
func ShowTables(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW TABLES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

func ShowCreateDatabase(db *sql.DB, database string) (string, error) {
	var oneRow [2]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1])
	}
	query := fmt.Sprintf("SHOW CREATE DATABASE `%s`", escapeString(database))
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", errors.WithMessage(err, query)
	}
	return oneRow[1], nil
}

func ShowCreateTable(db *sql.DB, database, table string) (string, error) {
	var oneRow [2]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1])
	}
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", escapeString(database), escapeString(table))
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", errors.WithMessage(err, query)
	}
	return oneRow[1], nil
}

func ShowCreateView(db *sql.DB, database, view string) (string, error) {
	var oneRow [4]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1], &oneRow[2], &oneRow[3])
	}
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", escapeString(database), escapeString(view))
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", errors.WithMessage(err, query)
	}
	return oneRow[1], nil
}

func ListAllTables(db *sql.DB, database string) ([]string, error) {
	var tables oneStrColumnTable
	const query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? and table_type = 'BASE TABLE'"
	if err := simpleQueryWithArgs(db, tables.handleOneRow, query, database); err != nil {
		return nil, errors.WithMessage(err, query)
	}
	return tables.data, nil
}

func ListAllViews(db *sql.DB, database string) ([]string, error) {
	var views oneStrColumnTable
	const query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? and table_type = 'VIEW'"
	if err := simpleQueryWithArgs(db, views.handleOneRow, query, database); err != nil {
		return nil, errors.WithMessage(err, query)
	}
	return views.data, nil
}

func SelectVersion(db *sql.DB) (string, error) {
	var versionInfo string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&versionInfo)
	}
	err := simpleQuery(db, "SELECT version()", handleOneRow)
	if err != nil {
		return "", withStack(err)
	}
	return versionInfo, nil
}

func SelectAllFromTable(conf *Config, db *sql.DB, database, table string) (TableDataIR, error) {
	selectedField, err := buildSelectField(db, database, table)
	if err != nil {
		return nil, err
	}

	colTypes, err := GetColumnTypes(db, selectedField, database, table)
	if err != nil {
		return nil, err
	}

	orderByClause, err := buildOrderByClause(conf, db, database, table)
	if err != nil {
		return nil, err
	}

	query := buildSelectQuery(database, table, selectedField, buildWhereCondition(conf, ""), orderByClause)
	rows, err := db.Query(query)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}

	return &tableData{
		database:        database,
		table:           table,
		rows:            rows,
		colTypes:        colTypes,
		selectedField:   selectedField,
		escapeBackslash: conf.EscapeBackslash,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, nil
}

func SelectFromSql(conf *Config, db *sql.DB) (TableDataIR, error) {
	rows, err := db.Query(conf.Sql)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, conf.Sql))
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, withStack(errors.WithMessage(err, conf.Sql))
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, withStack(errors.WithMessage(err, conf.Sql))
	}
	for i := range cols {
		cols[i] = wrapBackTicks(cols[i])
	}
	return &tableData{
		database:        "",
		table:           "",
		rows:            rows,
		colTypes:        colTypes,
		selectedField:   strings.Join(cols, ","),
		escapeBackslash: conf.EscapeBackslash,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, nil
}

func buildSelectQuery(database, table string, fields string, where string, orderByClause string) string {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(fields)
	query.WriteString(" FROM `")
	query.WriteString(escapeString(database))
	query.WriteString("`.`")
	query.WriteString(escapeString(table))
	query.WriteString("`")

	if where != "" {
		query.WriteString(" ")
		query.WriteString(where)
	}

	if orderByClause != "" {
		query.WriteString(" ")
		query.WriteString(orderByClause)
	}

	return query.String()
}

func buildOrderByClause(conf *Config, db *sql.DB, database, table string) (string, error) {
	if !conf.SortByPk {
		return "", nil
	}
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		ok, err := SelectTiDBRowID(db, database, table)
		if err != nil {
			return "", withStack(err)
		}
		if ok {
			return "ORDER BY _tidb_rowid", nil
		} else {
			return "", nil
		}
	}
	pkName, err := GetPrimaryKeyName(db, database, table)
	if err != nil {
		return "", withStack(err)
	}
	tableContainsPriKey := pkName != ""
	if tableContainsPriKey {
		return fmt.Sprintf("ORDER BY `%s`", escapeString(pkName)), nil
	}
	return "", nil
}

func SelectTiDBRowID(db *sql.DB, database, table string) (bool, error) {
	const errBadFieldCode = 1054
	tiDBRowIDQuery := fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", escapeString(database), escapeString(table))
	_, err := db.Exec(tiDBRowIDQuery)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, fmt.Sprintf("%d", errBadFieldCode)) {
			return false, nil
		}
		return false, withStack(errors.WithMessage(err, tiDBRowIDQuery))
	}
	return true, nil
}

func GetColumnTypes(db *sql.DB, fields, database, table string) ([]*sql.ColumnType, error) {
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT 1", fields, escapeString(database), escapeString(table))
	rows, err := db.Query(query)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}
	defer rows.Close()
	return rows.ColumnTypes()
}

func GetPrimaryKeyName(db *sql.DB, database, table string) (string, error) {
	priKeyQuery := "SELECT column_name FROM information_schema.columns " +
		"WHERE table_schema = ? AND table_name = ? AND column_key = 'PRI';"
	var colName string
	row := db.QueryRow(priKeyQuery, database, table)
	if err := row.Scan(&colName); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return "", withStack(errors.WithMessage(err, priKeyQuery))
		}
	}
	return colName, nil
}

func GetUniqueIndexName(db *sql.DB, database, table string) (string, error) {
	uniKeyQuery := "SELECT column_name FROM information_schema.columns " +
		"WHERE table_schema = ? AND table_name = ? AND column_key = 'UNI';"
	var colName string
	row := db.QueryRow(uniKeyQuery, database, table)
	if err := row.Scan(&colName); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return "", withStack(errors.WithMessage(err, uniKeyQuery))
		}
	}
	return colName, nil
}

func FlushTableWithReadLock(db *sql.DB) error {
	_, err := db.Exec("FLUSH TABLES WITH READ LOCK")
	return withStack(err)
}

func LockTables(db *sql.DB, database, table string) error {
	_, err := db.Exec(fmt.Sprintf("LOCK TABLES `%s`.`%s` READ", escapeString(database), escapeString(table)))
	return withStack(err)
}

func UnlockTables(db *sql.DB) error {
	_, err := db.Exec("UNLOCK TABLES")
	return withStack(err)
}

func UseDatabase(db *sql.DB, databaseName string) error {
	_, err := db.Exec(fmt.Sprintf("USE `%s`", escapeString(databaseName)))
	return withStack(err)
}

func ShowMasterStatus(db *sql.DB, fieldNum int) ([]string, error) {
	oneRow := make([]string, fieldNum)
	addr := make([]interface{}, fieldNum)
	for i := range oneRow {
		addr[i] = &oneRow[i]
	}
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(addr...)
	}
	err := simpleQuery(db, "SHOW MASTER STATUS", handleOneRow)
	if err != nil {
		return nil, err
	}
	return oneRow, nil
}

func GetSpecifiedColumnValue(rows *sql.Rows, columnName string) ([]string, error) {
	var strs []string
	columns, _ := rows.Columns()
	addr := make([]interface{}, len(columns))
	oneRow := make([]sql.NullString, len(columns))
	var fieldIndex = -1
	for i, col := range columns {
		if strings.ToUpper(col) == columnName {
			fieldIndex = i
		}
		addr[i] = &oneRow[i]
	}
	if fieldIndex == -1 {
		return strs, nil
	}
	for rows.Next() {
		err := rows.Scan(addr...)
		if err != nil {
			return strs, err
		}
		strs = append(strs, oneRow[fieldIndex].String)
	}
	return strs, nil
}

func GetPdAddrs(db *sql.DB) ([]string, error) {
	query := "SELECT * FROM information_schema.cluster_info where type = 'pd';"
	rows, err := db.Query(query)
	if err != nil {
		log.Warn("can't execute query from db",
			zap.String("query", query), zap.Error(err))
		return []string{}, err
	}
	defer rows.Close()
	return GetSpecifiedColumnValue(rows, "STATUS_ADDRESS")
}

func GetTiDBDDLIDs(db *sql.DB) ([]string, error) {
	query := "SELECT * FROM information_schema.tidb_servers_info;"
	rows, err := db.Query(query)
	if err != nil {
		log.Warn("can't execute query from db",
			zap.String("query", query), zap.Error(err))
		return []string{}, err
	}
	defer rows.Close()
	return GetSpecifiedColumnValue(rows, "DDL_ID")
}

func CheckTiDBWithTiKV(db *sql.DB) (bool, error) {
	var count int
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&count)
	}
	err := simpleQuery(db, "SELECT COUNT(1) as c FROM MYSQL.TiDB WHERE VARIABLE_NAME='tikv_gc_safe_point'", handleOneRow)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func getSnapshot(db *sql.DB) (string, error) {
	str, err := ShowMasterStatus(db, showMasterStatusFieldNum)
	if err != nil {
		return "", err
	}
	return str[snapshotFieldIndex], nil
}

func isUnknownSystemVariableErr(err error) bool {
	return strings.Contains(err.Error(), "Unknown system variable")
}

func resetDBWithSessionParams(db *sql.DB, dsn string, params map[string]string) (*sql.DB, error) {
	support := make(map[string]string)
	for k, v := range params {
		v = wrapQuotes(v)
		s := fmt.Sprintf("SET SESSION %s = %s", k, v)
		_, err := db.Exec(s)
		if err != nil {
			if isUnknownSystemVariableErr(err) {
				log.Info("session variable is not supported by db", zap.String("variable", k), zap.String("value", v))
				continue
			}
			return nil, withStack(err)
		}

		support[k] = v
	}

	for k, v := range support {
		dsn += fmt.Sprintf("&%s=%s", k, url.QueryEscape(v))
	}

	newDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, withStack(err)
	}

	db.Close()

	return newDB, nil
}

func buildSelectField(db *sql.DB, dbName, tableName string) (string, error) {
	query := `SELECT COLUMN_NAME,EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?;`
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	availableFields := make([]string, 0)

	hasGenerateColumn := false
	var fieldName string
	var extra string
	for rows.Next() {
		err = rows.Scan(&fieldName, &extra)
		if err != nil {
			return "", withStack(errors.WithMessage(err, query))
		}
		switch extra {
		case "STORED GENERATED", "VIRTUAL GENERATED":
			hasGenerateColumn = true
			continue
		}
		availableFields = append(availableFields, wrapBackTicks(escapeString(fieldName)))
	}
	if hasGenerateColumn {
		return strings.Join(availableFields, ","), nil
	}
	return "*", nil
}

type oneStrColumnTable struct {
	data []string
}

func (o *oneStrColumnTable) handleOneRow(rows *sql.Rows) error {
	var str string
	if err := rows.Scan(&str); err != nil {
		return withStack(err)
	}
	o.data = append(o.data, str)
	return nil
}

func simpleQuery(db *sql.DB, sql string, handleOneRow func(*sql.Rows) error) error {
	return simpleQueryWithArgs(db, handleOneRow, sql)
}

func simpleQueryWithArgs(db *sql.DB, handleOneRow func(*sql.Rows) error, sql string, args ...interface{}) error {
	rows, err := db.Query(sql, args...)
	if err != nil {
		return withStack(err)
	}

	for rows.Next() {
		if err := handleOneRow(rows); err != nil {
			rows.Close()
			return withStack(err)
		}
	}
	return nil
}

func pickupPossibleField(dbName, tableName string, db *sql.DB, conf *Config) (string, error) {
	// If detected server is TiDB, try using _tidb_rowid
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		ok, err := SelectTiDBRowID(db, dbName, tableName)
		if err != nil {
			return "", nil
		}
		if ok {
			return "_tidb_rowid", nil
		}
	}
	// try to use pk
	fieldName, err := GetPrimaryKeyName(db, dbName, tableName)
	if err != nil {
		return "", err
	}
	// try to use first uniqueIndex
	if fieldName == "" {
		fieldName, err = GetUniqueIndexName(db, dbName, tableName)
		if err != nil {
			return "", err
		}
	}

	// there is no proper index
	if fieldName == "" {
		return "", nil
	}

	query := "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
		"WHERE TABLE_NAME = ? AND COLUMN_NAME = ?"
	var fieldType string
	row := db.QueryRow(query, tableName, fieldName)
	err = row.Scan(&fieldType)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return "", withStack(errors.WithMessage(err, query))
		}
	}
	switch strings.ToLower(fieldType) {
	case "int", "bigint":
		return fieldName, nil
	}
	return "", nil
}

func estimateCount(dbName, tableName string, db *sql.DB, field string, conf *Config) uint64 {
	query := fmt.Sprintf("EXPLAIN SELECT `%s` FROM `%s`.`%s`", field, escapeString(dbName), escapeString(tableName))

	if conf.Where != "" {
		query += " WHERE "
		query += conf.Where
	}

	estRows := detectEstimateRows(db, query, []string{"rows", "estRows", "count"})
	/* tidb results field name is estRows (before 4.0.0-beta.2: count)
		+-----------------------+----------+-----------+---------------------------------------------------------+
		| id                    | estRows  | task      | access object | operator info                           |
		+-----------------------+----------+-----------+---------------------------------------------------------+
		| tablereader_5         | 10000.00 | root      |               | data:tablefullscan_4                    |
		| └─tablefullscan_4     | 10000.00 | cop[tikv] | table:a       | table:a, keep order:false, stats:pseudo |
		+-----------------------+----------+-----------+----------------------------------------------------------

	mariadb result field name is rows
		+------+-------------+---------+-------+---------------+------+---------+------+----------+-------------+
		| id   | select_type | table   | type  | possible_keys | key  | key_len | ref  | rows     | Extra       |
		+------+-------------+---------+-------+---------------+------+---------+------+----------+-------------+
		|    1 | SIMPLE      | sbtest1 | index | NULL          | k_1  | 4       | NULL | 15000049 | Using index |
		+------+-------------+---------+-------+---------------+------+---------+------+----------+-------------+

	mysql result field name is rows
		+----+-------------+-------+------------+-------+---------------+-----------+---------+------+------+----------+-------------+
		| id | select_type | table | partitions | type  | possible_keys | key       | key_len | ref  | rows | filtered | Extra       |
		+----+-------------+-------+------------+-------+---------------+-----------+---------+------+------+----------+-------------+
		|  1 | SIMPLE      | t1    | NULL       | index | NULL          | multi_col | 10      | NULL |    5 |   100.00 | Using index |
		+----+-------------+-------+------------+-------+---------------+-----------+---------+------+------+----------+-------------+
	*/
	if estRows > 0 {
		return estRows
	}
	return 0
}

func detectEstimateRows(db *sql.DB, query string, fieldNames []string) uint64 {
	row, err := db.Query(query)
	if err != nil {
		log.Warn("can't execute query from db",
			zap.String("query", query), zap.Error(err))
		return 0
	}
	row.Next()
	columns, _ := row.Columns()
	addr := make([]interface{}, len(columns))
	oneRow := make([]sql.NullString, len(columns))
	var fieldIndex = -1
	for i := range oneRow {
		addr[i] = &oneRow[i]
	}
found:
	for i := range oneRow {
		for _, fieldName := range fieldNames {
			if strings.EqualFold(columns[i], fieldName) {
				fieldIndex = i
				break found
			}
		}
	}
	err = row.Scan(addr...)
	if err != nil || fieldIndex < 0 {
		log.Warn("can't get estimate count from db",
			zap.String("query", query), zap.Error(err))
		return 0
	}

	estRows, err := strconv.ParseFloat(oneRow[fieldIndex].String, 64)
	if err != nil {
		log.Warn("can't get parse rows from db",
			zap.String("query", query), zap.Error(err))
		return 0
	}
	return uint64(estRows)
}

func buildWhereCondition(conf *Config, where string) string {
	var query strings.Builder
	separator := "WHERE"
	if conf.Where != "" {
		query.WriteString(" ")
		query.WriteString(separator)
		query.WriteString(" ")
		query.WriteString(conf.Where)
		separator = "AND"
	}
	if where != "" {
		query.WriteString(" ")
		query.WriteString(separator)
		query.WriteString(" ")
		query.WriteString(where)
	}
	return query.String()
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}
