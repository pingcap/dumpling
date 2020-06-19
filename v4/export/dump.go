package export

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/dumpling/v4/log"

	_ "github.com/go-sql-driver/mysql"
	pd "github.com/pingcap/pd/v4/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func Dump(pCtx context.Context, conf *Config) (err error) {
	if err = adjustConfig(conf); err != nil {
		return withStack(err)
	}

	go func() {
		if conf.StatusAddr != "" {
			err1 := startDumplingService(conf.StatusAddr)
			if err1 != nil {
				log.Error("dumpling stops to serving service", zap.Error(err1))
			}
		}
	}()
	pool, err := sql.Open("mysql", conf.getDSN(""))
	if err != nil {
		return withStack(err)
	}
	defer pool.Close()

	conf.ServerInfo, err = detectServerInfo(pool)
	if err != nil {
		if strings.Contains(err.Error(), "tidb_mem_quota_query") {
			conf.TiDBMemQuotaQuery = UnspecifiedSize
			pool, err = sql.Open("mysql", conf.getDSN(""))
			if err != nil {
				return withStack(err)
			}
			conf.ServerInfo, err = detectServerInfo(pool)
			if err != nil {
				return withStack(err)
			}
		} else {
			return withStack(err)
		}
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

	filterTables(conf)

	ctx, cancel := context.WithCancel(pCtx)
	defer cancel()

	var doPdGC bool
	var pdClient pd.Client
	if conf.ServerInfo.ServerType == ServerTypeTiDB && conf.ServerInfo.ServerVersion.Compare(*gcSafePointVersion) >= 0 {
		pdAddrs, err := GetPdAddrs(pool)
		if err != nil {
			return err
		}
		if len(pdAddrs) > 0 {
			doPdGC, err = checkSameCluster(ctx, pool, pdAddrs)
			if err != nil {
				log.Warn("meet error while check whether fetched pd addr and TiDB belongs to one cluster", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
			} else if doPdGC {
				pdClient, err = pd.NewClientWithContext(ctx, pdAddrs, pd.SecurityOption{})
				if err != nil {
					log.Warn("create pd client to control GC failed", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
					doPdGC = false
				}
			}
		}
	}

	if conf.Snapshot == "" && (doPdGC || conf.Consistency == "flush") {
		if conf.Snapshot == "" {
			str, err := ShowMasterStatus(pool, showMasterStatusFieldNum)
			if err != nil {
				return err
			}
			conf.Snapshot = str[snapshotFieldIndex]
		}
	}

	if doPdGC {
		snapshotTS, err := strconv.ParseUint(conf.Snapshot, 10, 64)
		if err != nil {
			return err
		}
		go updateServiceSafePoint(ctx, pdClient, defaultDumpGCSafePointTTL, snapshotTS)
	} else if conf.ServerInfo.ServerType == ServerTypeTiDB {
		log.Warn("If the amount of data to dump is large, criteria: (data more than 60GB or dumped time more than 10 minutes)\n" +
			"you'd better adjust the tikv_gc_life_time to avoid export failure due to TiDB GC during the dump process.\n" +
			"Before dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '720h' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n" +
			"After dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '10m' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n")
	}

	conCtrl, err := NewConsistencyController(conf, pool)
	if err != nil {
		return err
	}
	if err = conCtrl.Setup(); err != nil {
		return err
	}

	m := newGlobalMetadata(conf.OutputDirPath)
	// write metadata even if dump failed
	defer m.writeGlobalMetaData()
	m.recordStartTime(time.Now())
	err = m.getGlobalMetaData(pool, conf.ServerInfo.ServerType)
	if err != nil {
		log.Info("get global metadata failed", zap.Error(err))
	}

	var writer Writer
	switch strings.ToLower(conf.FileType) {
	case "sql":
		writer, err = NewSimpleWriter(conf)
	case "csv":
		writer, err = NewCsvWriter(conf)
	}
	if err != nil {
		return err
	}

	if conf.Sql == "" {
		if err = dumpDatabases(ctx, conf, pool, writer); err != nil {
			return err
		}
	} else {
		if err = dumpSql(ctx, conf, pool, writer); err != nil {
			return err
		}
	}

	m.recordFinishTime(time.Now())

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

		if len(tables) == 0 {
			continue
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
	}
	return nil
}

func dumpSql(ctx context.Context, conf *Config, db *sql.DB, writer Writer) error {
	tableIR, err := SelectFromSql(conf, db)
	if err != nil {
		return err
	}

	return writer.WriteTableData(ctx, tableIR)
}

func dumpTable(ctx context.Context, conf *Config, db *sql.DB, dbName string, table *TableInfo, writer Writer) error {
	tableName := table.Name
	if !conf.NoSchemas {
		if table.Type == TableTypeView {
			viewName := table.Name
			createViewSQL, err := ShowCreateView(db, dbName, viewName)
			if err != nil {
				return err
			}
			return writer.WriteTableMeta(ctx, dbName, viewName, createViewSQL)
		}
		createTableSQL, err := ShowCreateTable(db, dbName, tableName)
		if err != nil {
			return err
		}
		if err := writer.WriteTableMeta(ctx, dbName, tableName, createTableSQL); err != nil {
			return err
		}
	}
	// Do not dump table data and return nil
	if conf.NoData {
		return nil
	}

	if conf.Rows != UnspecifiedSize {
		finished, err := concurrentDumpTable(ctx, writer, conf, db, dbName, tableName)
		if err != nil || finished {
			return err
		}
	}
	tableIR, err := SelectAllFromTable(conf, db, dbName, tableName)
	if err != nil {
		return err
	}

	return writer.WriteTableData(ctx, tableIR)
}

func concurrentDumpTable(ctx context.Context, writer Writer, conf *Config, db *sql.DB, dbName string, tableName string) (bool, error) {
	// try dump table concurrently by split table to chunks
	chunksIterCh := make(chan TableDataIR, defaultDumpThreads)
	errCh := make(chan error, defaultDumpThreads)
	linear := make(chan struct{})

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	var g errgroup.Group
	g.Go(func() error {
		splitTableDataIntoChunks(ctx1, chunksIterCh, errCh, linear, dbName, tableName, db, conf)
		return nil
	})

Loop:
	for {
		select {
		case <-ctx.Done():
			return true, nil
		case <-linear:
			return false, nil
		case chunksIter, ok := <-chunksIterCh:
			if !ok {
				break Loop
			}
			g.Go(func() error {
				return writer.WriteTableData(ctx, chunksIter)
			})
		case err := <-errCh:
			return false, err
		}
	}
	if err := g.Wait(); err != nil {
		return true, err
	}
	return true, nil
}

func updateServiceSafePoint(ctx context.Context, pdClient pd.Client, ttl int64, snapshotTS uint64) {
	updateInterval := time.Duration(ttl/2) * time.Second
	tick := time.NewTicker(updateInterval)

	for {
		log.Debug("update PD safePoint limit with ttl",
			zap.Uint64("safePoint", snapshotTS),
			zap.Int64("ttl", ttl))
		for retryCnt := 0; retryCnt <= 10; retryCnt++ {
			_, err := pdClient.UpdateServiceGCSafePoint(ctx, dumplingServiceSafePointID, ttl, snapshotTS)
			if err == nil {
				break
			}
			log.Debug("update PD safePoint failed", zap.Error(err), zap.Int("retryTime", retryCnt))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
