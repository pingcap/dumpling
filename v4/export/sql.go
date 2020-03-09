package export

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
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
	query := fmt.Sprintf("SHOW CREATE DATABASE %s", database)
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
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, table)
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
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, view)
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", errors.WithMessage(err, query)
	}
	return oneRow[1], nil
}

func ListAllTables(db *sql.DB, database string) ([]string, error) {
	var tables oneStrColumnTable
	const query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' and table_type = 'BASE TABLE'"
	if err := simpleQuery(db, fmt.Sprintf(query, database), tables.handleOneRow); err != nil {
		return nil, errors.WithMessage(err, query)
	}
	return tables.data, nil
}

func ListAllViews(db *sql.DB, database string) ([]string, error) {
	var views oneStrColumnTable
	const query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' and table_type = 'VIEW'"
	if err := simpleQuery(db, fmt.Sprintf(query, database), views.handleOneRow); err != nil {
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
	colTypes, err := GetColumnTypes(db, database, table)
	if err != nil {
		return nil, err
	}

	query, err := buildSelectAllQuery(conf, db, database, table, "")
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}

	return &tableData{
		database: database,
		table:    table,
		rows:     rows,
		colTypes: colTypes,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, nil
}

func buildSelectAllQuery(conf *Config, db *sql.DB, database, table string, where string) (string, error) {
	var query strings.Builder
	query.WriteString("SELECT * FROM ")
	query.WriteString(database)
	query.WriteString(".")
	query.WriteString(table)

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

	if conf.SortByPk {
		orderByClause, err := buildOrderByClause(conf, db, database, table)
		if err != nil {
			return "", err
		}
		if orderByClause != "" {
			query.WriteString(" ")
			query.WriteString(orderByClause)
		}
	}

	return query.String(), nil
}

func buildOrderByClause(conf *Config, db *sql.DB, database, table string) (string, error) {
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		ok, err := SelectTiDBRowID(db, database, table)
		if err != nil {
			return "", err
		}
		if ok {
			return "ORDER BY _tidb_rowid", nil
		} else {
			return "", nil
		}
	}
	pkName, err := GetPrimaryKeyName(db, database, table)
	if err != nil {
		return "", err
	}
	tableContainsPriKey := pkName != ""
	if tableContainsPriKey {
		return fmt.Sprintf("ORDER BY %s", pkName), nil
	}
	return "", nil
}

func SelectTiDBRowID(db *sql.DB, database, table string) (bool, error) {
	const errBadFieldCode = 1054
	tiDBRowIDQuery := fmt.Sprintf("SELECT _tidb_rowid from %s.%s LIMIT 0", database, table)
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

func GetColumnTypes(db *sql.DB, database, table string) ([]*sql.ColumnType, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", database, table)
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", database, table))
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}
	defer rows.Close()
	return rows.ColumnTypes()
}

func GetPrimaryKeyName(db *sql.DB, database, table string) (string, error) {
	priKeyQuery := `SELECT column_name FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_key = 'PRI';`
	stmt, err := db.Prepare(priKeyQuery)
	if err != nil {
		return "", err
	}
	defer stmt.Close()
	rows, err := stmt.Query(database, table)
	if err != nil {
		return "", withStack(errors.WithMessage(err, priKeyQuery))
	}

	var colName string
	for rows.Next() {
		if err := rows.Scan(&colName); err != nil {
			rows.Close()
			return "", withStack(err)
		}
	}
	return colName, nil
}

func GetUniqueIndexName(db *sql.DB, database, table string) (string, error) {
	uniKeyQuery := `SELECT column_name FROM information_schema.columns 
		WHERE table_schema = ? AND table_name = ? AND column_key = 'UNI';`
	stmt, err := db.Prepare(uniKeyQuery)
	if err != nil {
		return "", err
	}
	defer stmt.Close()
	rows, err := stmt.Query(database, table)
	if err != nil {
		return "", withStack(errors.WithMessage(err, uniKeyQuery))
	}

	var colName string
	for rows.Next() {
		if err := rows.Scan(&colName); err != nil {
			rows.Close()
			return "", withStack(err)
		}
	}
	return colName, nil
}

func FlushTableWithReadLock(db *sql.DB) error {
	_, err := db.Exec("FLUSH TABLES WITH READ LOCK")
	return withStack(err)
}

func LockTables(db *sql.DB, database, table string) error {
	_, err := db.Exec(fmt.Sprintf("LOCK TABLES `%s`.`%s` READ", database, table))
	return withStack(err)
}

func UnlockTables(db *sql.DB) error {
	_, err := db.Exec("UNLOCK TABLES")
	return withStack(err)
}

func UseDatabase(db *sql.DB, databaseName string) error {
	_, err := db.Exec(fmt.Sprintf("USE %s", databaseName))
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

func SetTiDBSnapshot(db *sql.DB, snapshot string) error {
	_, err := db.Exec("SET SESSION tidb_snapshot = ?", snapshot)
	return withStack(err)
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
	rows, err := db.Query(sql)
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
		query := fmt.Sprintf("SELECT _tidb_rowid FROM `%s`.`%s` LIMIT 0", dbName, tableName)
		handleOneRow := func(rows *sql.Rows) error {
			return rows.Err()
		}
		err := simpleQuery(db, query, handleOneRow)
		if err == nil {
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
	}

	// there is no proper index
	if fieldName == "" {
		return "", nil
	}

	query := fmt.Sprintf("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "+
		"WHERE table_name = '%s' AND COLUMN_NAME = '%s'", tableName, fieldName)
	var fieldType string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&fieldType)
	}
	err = simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", err
	}
	switch strings.ToLower(fieldType) {
	case "int", "bigint":
		return fieldName, nil
	}
	return "", nil
}

func estimateCount(dbName, tableName string, db *sql.DB, field string, conf *Config) uint64 {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("EXPLAIN SELECT `%s` FROM `%s`.`%s`", field, dbName, tableName))

	if conf.Where != "" {
		query.WriteString(" WHERE")
		query.WriteString(conf.Where)
	}

	var estRows float64
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		oneRow := make([]string, 4)
		addr := make([]interface{}, 4)
		for i := range oneRow {
			addr[i] = &oneRow[i]
		}
		handleOneRow := func(rows *sql.Rows) error {
			return rows.Scan(addr...)
		}
		err := simpleQuery(db, query.String(), handleOneRow)
		if err != nil {
			return 0
		}
		/* tidb results field name is estRows
		+-----------------------+----------+-----------+-----------------------------------------+
		| id                    | estRows  | task      | operator info                           |
		+-----------------------+----------+-----------+-----------------------------------------+
		| TableReader_5         | 10000.00 | root      | data:TableFullScan_4                    |
		| └─TableFullScan_4     | 10000.00 | cop[tikv] | table:a, keep order:false, stats:pseudo |
		+-----------------------+----------+-----------+----------------------------------------
		*/

		estRows, err = strconv.ParseFloat(oneRow[1], 64)
		if err != nil {
			return 0
		}
	} else {
		/* mysql result field name is rows
		+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-------+
		| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra |
		+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-------+
		|  1 | SIMPLE      | a     | NULL       | ALL  | NULL          | NULL | NULL    | NULL |    1 |   100.00 | NULL  |
		+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-------+
		*/
		oneRow := make([]string, 12)
		addr := make([]interface{}, 12)
		for i := range oneRow {
			addr[i] = &oneRow[i]
		}
		handleOneRow := func(rows *sql.Rows) error {
			return rows.Scan(addr...)
		}
		err := simpleQuery(db, query.String(), handleOneRow)
		if err != nil {
			return 0
		}
		estRows, err = strconv.ParseFloat(oneRow[9], 64)
		if err != nil {
			return 0
		}
	}
	if estRows > 0 {
		return uint64(estRows)
	}

	return 0
}
