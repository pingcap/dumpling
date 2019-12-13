package dumpling

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type dumper struct {
	conn   *sql.DB
	writer Writer
}

func Dump() error {
	if err := dumpDatabase("mysql", &dummyWriter{}); err != nil {
		return err
	}
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

func showDatabases(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW DATABASES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

// showTables shows the tables of a database, the caller should use the correct database.
func showTables(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW TABLES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

func showCreateTable(db *sql.DB, database, table string) (string, error) {
	var oneRow [2]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1])
	}
	sql := fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, table)
	err := simpleQuery(db, sql, handleOneRow)
	if err != nil {
		return "", err
	}
	return oneRow[1], nil
}

func dumpDatabase(dbName string, writer Writer) error {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/"+dbName)
	if err != nil {
		return withStack(err)
	}
	defer db.Close()

	writer.WriteDatabaseMeta(dbName)

	tables, err := showTables(db)
	if err != nil {
		return err
	}

	for _, table := range tables {
		createTableSQL, err := showCreateTable(db, dbName, table)
		if err != nil {
			return err
		}

		writer.WriteTableMeta(dbName, table, createTableSQL)

		dumper := writer.NewTableDumper(dbName, table)
		if err := dumpTable(db, table, dumper); err != nil {
			return err
		}
	}
	return nil
}

func getColumnTypes(db *sql.DB, table string) ([]*sql.ColumnType, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
	if err != nil {
		return nil, withStack(err)
	}
	defer rows.Close()
	return rows.ColumnTypes()
}

type tableDumper interface {
	handleOneRow(rows *sql.Rows) error
	prepareColumns(colTypes []*sql.ColumnType)
	finishTable()
}

func dumpTable(db *sql.DB, table string, dumper tableDumper) error {
	fmt.Println("dumpling table:", table)
	colTypes, err := getColumnTypes(db, table)
	if err != nil {
		return err
	}
	dumper.prepareColumns(colTypes)

	rows, err := db.Query(fmt.Sprintf("SELECT * from %s", table))
	for rows.Next() {
		if err := dumper.handleOneRow(rows); err != nil {
			rows.Close()
			return err
		}
	}
	dumper.finishTable()
	return nil
}

type scanProvider interface {
	prepareScanArgs([]interface{})
}

func decodeFromRows(rows *sql.Rows, args []interface{}, row scanProvider) error {
	row.prepareScanArgs(args)
	if err := rows.Scan(args...); err != nil {
		return withStack(err)
	}
	return nil
}
