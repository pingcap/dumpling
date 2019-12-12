package dumpling

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type dumper struct {
	conn   *sql.DB
	writer Writer
}

func Dump() error {
	d, err := prepareDump()
	if err != nil {
		return err
	}

	err = d.dumpMeta()
	if err != nil {
		return err
	}

	err = d.dumpData()
	if err != nil {
		return err
	}

	return nil
}

func prepareDump() (*dumper, error) {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/")
	if err != nil {
		return nil, err
	}
	// TODO: Set timezone, snapshot etc.
	return &dumper{
		conn:   db,
		writer: &dummyWriter{},
	}, nil
}

func (d *dumper) dumpMeta() error {
	rows, err := d.conn.Query("SHOW DATABASES")
	if err != nil {
		return withStack(err)
	}

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return withStack(err)
		}
		databases = append(databases, name)
	}

	return d.dumpDatabase(databases)
}

func (d *dumper) dumpDatabase(databases []string) error {
	err := d.writer.WriteDatabases(databases)
	if err != nil {
		return err
	}

	for _, database := range databases {
		if err := d.dumpTablesMeta(database); err != nil {
			return err
		}
	}
	return nil
}

func (d *dumper) dumpTablesMeta(database string) error {
	conn, err := d.conn.Conn(context.Background())
	if err != nil {
		return withStack(err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), fmt.Sprintf("USE %s", database))
	if err != nil {
		return withStack(err)
	}

	rows, err := conn.QueryContext(context.Background(), "SHOW TABLES")
	if err != nil {
		return withStack(err)
	}
	for rows.Next() {
		var table string
		if rows.Scan(&table) != nil {
			rows.Close()
			return withStack(err)
		}

		if err := d.dumpTableMeta(database, table); err != nil {
			return err
		}
	}
	return nil
}

func (d *dumper) dumpTableMeta(db, table string) error {
	rows := d.conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s.%s", db, table))
	var str string
	if err := rows.Scan(&table, &str); err != nil {
		return withStack(err)
	}

	if err := d.writer.WriteTable(str); err != nil {
		return err
	}
	return nil
}

func (d *dumper) dumpData() error {
	return nil
}

func (d *dumper) dumpTable(table string) error {
	rows, err := d.conn.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
	if err != nil {
		return withStack(err)
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return withStack(err)
	}
	rows.Close()

	var dst tableData
	// TODO: Implement Scanner for []types.Datum to avoid convertion cost.
	res := make([]interface{}, len(colTypes))
	rows, err = d.conn.Query(fmt.Sprintf("SELECT * from %s", table))
	for rows.Next() {
		row := make([]string, len(colTypes))
		for i := 0; i < len(colTypes); i++ {
			res[i] = &row[i]
		}

		err = rows.Scan(res...)
		if err != nil {
			rows.Close()
			return withStack(err)
		}

		for _, s := range row {
			dst.size += len(s)
		}
		dst.rows = append(dst.rows, row)
		const chunkSize = 4 << 20
		if dst.size > chunkSize {
			dst.size = 0
			fmt.Println("split chunk")
			d.writer.WriteData()
		}
	}
	return nil
}

type tableData struct {
	size int
	rows [][]string
}
