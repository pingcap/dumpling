package export

import (
	"context"
	"database/sql"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

func Dump(conf *Config) (err error) {
	pool, err := sql.Open("mysql", conf.getDSN(""))
	if err != nil {
		return withStack(err)
	}
	defer pool.Close()

	conf.ServerInfo, err = detectServerInfo(pool)
	if err != nil {
		return err
	}

	databases, err := prepareDumpingDatabases(conf, pool)
	if err != nil {
		return err
	}

	conf.Tables, err = listAllTables(pool, databases)
	if err != nil {
		return err
	}

	conCtrl := ConsistencyController{}
	err = conCtrl.Setup(conf, ConsistencyNone)
	if err != nil {
		return err
	}

	fsWriter, err := NewSimpleWriter(conf)
	if err != nil {
		return err
	}

	if err = dumpDatabases(context.Background(), conf, pool, fsWriter); err != nil {
		return err
	}
	return conCtrl.TearDown()
}

func dumpDatabases(ctx context.Context, conf *Config, db *sql.DB, writer Writer) error {
	allTables := conf.Tables
	for dbName, tables := range allTables {
		createDatabaseSQL, err := ShowCreateDatabase(db, dbName)
		if err != nil {
			return err
		}
		if err := writer.WriteDatabaseMeta(ctx, dbName, createDatabaseSQL); err != nil {
			return err
		}

		rateLimit := newRateLimit(conf.Threads)
		var wg sync.WaitGroup
		wg.Add(len(tables))
		res := make([]error, len(tables))
		for i, table := range tables {
			go func(ith int, table string, wg *sync.WaitGroup, res []error) {
				defer wg.Done()
				rateLimit.getToken()
				defer rateLimit.putToken()
				res[ith] = dumpTable(ctx, conf, db, dbName, table, writer)
			}(i, table, &wg, res)
		}
		wg.Wait()
		for _, err := range res {
			if err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func dumpTable(ctx context.Context, conf *Config, db *sql.DB, dbName, table string, writer Writer) error {
	createTableSQL, err := ShowCreateTable(db, dbName, table)
	if err != nil {
		return err
	}
	if err := writer.WriteTableMeta(ctx, dbName, table, createTableSQL); err != nil {
		return err
	}

	tableIR, err := SelectAllFromTable(conf, db, dbName, table)
	if err != nil {
		return err
	}

	if err := writer.WriteTableData(ctx, tableIR); err != nil {
		return err
	}
	return nil
}
