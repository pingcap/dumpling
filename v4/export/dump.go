package export

import (
	"context"
	"database/sql"
	"fmt"
	"path"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/sync/errgroup"
)

func Dump(conf *Config) (err error) {
	pool, err := sql.Open("mysql", conf.getDSN(""))
	if err != nil {
		return withStack(err)
	}
	defer pool.Close()

	adjustConfig(conf)

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

	if !conf.NoViews {
		views, err := listAllViews(pool, databases)
		if err != nil {
			return err
		}
		conf.Tables.Merge(views)
	}

	err = filterTables(conf)
	if err != nil {
		return err
	}

	conCtrl, err := NewConsistencyController(conf, pool)
	if err != nil {
		return err
	}
	if err = conCtrl.Setup(); err != nil {
		return err
	}

	writer, err := NewSimpleWriter(conf)
	if err != nil {
		return err
	}
	if err = dumpDatabases(context.Background(), conf, pool, writer); err != nil {
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
		var g errgroup.Group
		for _, table := range tables {
			table := table
			g.Go(func() error {
				rateLimit.getToken()
				defer rateLimit.putToken()
				return dumpTable(ctx, conf, db, dbName, table, writer)
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func dumpTable(ctx context.Context, conf *Config, db *sql.DB, dbName string, table *TableInfo, writer Writer) error {
	if table.Type == TableTypeView {
		viewName := table.Name
		createViewSQL, err := ShowCreateView(db, dbName, viewName)
		if err != nil {
			return err
		}
		return writer.WriteTableMeta(ctx, dbName, viewName, createViewSQL)
	}

	tableName := table.Name
	createTableSQL, err := ShowCreateTable(db, dbName, tableName)
	if err != nil {
		return err
	}
	if err := writer.WriteTableMeta(ctx, dbName, tableName, createTableSQL); err != nil {
		return err
	}

	if conf.Rows != UnspecifiedSize {
		concurrentDumpSkipped, err = concurrentDumpTable(ctx, conf, db, dbName, tableName)
		if err != nil {
			return err
		}
		if  dumpFinished {
		    return nil
		}
	}

	if concurrentDumpSkipped {
		tableIR, err := SelectAllFromTable(conf, db, dbName, tableName)
		if err != nil {
			return err
		}

		if err := writer.WriteTableData(ctx, tableIR); err != nil {
			return err
		}
	}
	return nil
}

func concurrentDumpTable(ctx context.Context, conf *Config, db *sql.DB, dbName string, tableName string) (bool, error) {
	// try dump table concurrently by split table to chunks
	chunksCh := make(chan *tableDataChunks, defaultDumpThreads)
	errCh := make(chan error, defaultDumpThreads)
	skipCh := make(chan struct{})

	var g errgroup.Group
	g.Go(func() error {
		splitTableDataIntoChunks(ctx, chunksCh, errCh, skipCh, dbName, tableName, db, conf)
		return nil
	})

Loop:
	for {
		select {
		case <- ctx.Done():
			return false, nil
		case chunk, ok := <-chunksCh:
			if ok {
				g.Go(func() error {
					fileName := fmt.Sprintf("%s.%s.%d.sql", dbName, tableName, chunk.offset)
					filePath := path.Join(conf.OutputDirPath, fileName)
					fileWriter, tearDown := buildLazyFileWriter(filePath)
					intWriter := &InterceptStringWriter{StringWriter: fileWriter}
					err := WriteInsert(chunk, intWriter)
					defer chunk.Rows().Close()
					tearDown()
					if err != nil {
						return err
					}
					if !intWriter.SomethingIsWritten {
						return nil
					}
					return nil
				})
			} else {
				break Loop
			}
		case err := <-errCh:
			return false, err
		case <-skipCh:
			return true, nil
		}
	}
	if err := g.Wait(); err != nil {
		return false, err
	}
	return false, nil
}
