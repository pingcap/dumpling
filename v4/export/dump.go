package export

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/pingcap/dumpling/v4/log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type Dumper struct {
	ctx       context.Context
	conf      *Config
	cancelCtx context.CancelFunc

	extStore storage.ExternalStorage
	dbHandle *sql.DB

	tidbPDClientForGC pd.Client
}

func NewDumper(ctx context.Context, conf *Config) (*Dumper, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	d := &Dumper{
		ctx:       ctx,
		conf:      conf,
		cancelCtx: cancelFn,
	}
	err := adjustConfig(conf,
		initLogger,
		registerTLSConfig,
		validateSpecifiedSQL)
	if err != nil {
		return nil, err
	}
	err = runSteps(d,
		createExternalStore,
		startHttpService,
		openSQLDB,
		detectServerInfo,
		resolveAutoConsistency,

		tidbSetPDClientForGC,
		tidbGetSnapshot,
		tidbStartGCSavepointUpdateService,

		setSessionParam)
	return d, err
}

func (d *Dumper) Dump() (err error) {
	ctx, conf, pool := d.ctx, d.conf, d.dbHandle
	m := newGlobalMetadata(d.extStore, conf.Snapshot)
	defer func() {
		if err == nil {
			_ = m.writeGlobalMetaData(ctx)
		}
	}()

	// for consistency lock, we should lock tables at first to get the tables we want to lock & dump
	// for consistency lock, record meta pos before lock tables because other tables may still be modified while locking tables
	if conf.Consistency == consistencyTypeLock {
		conn, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			return errors.Trace(err)
		}
		m.recordStartTime(time.Now())
		err = m.recordGlobalMetaData(conn, conf.ServerInfo.ServerType, false)
		if err != nil {
			log.Info("get global metadata failed", zap.Error(err))
		}
		if err = prepareTableListToDump(conf, conn); err != nil {
			conn.Close()
			return err
		}
		conn.Close()
	}

	conCtrl, err := NewConsistencyController(ctx, conf, pool)
	if err != nil {
		return err
	}
	if err = conCtrl.Setup(ctx); err != nil {
		return err
	}
	// To avoid lock is not released
	defer conCtrl.TearDown(ctx)

	// for other consistencies, we should get table list after consistency is set up and GlobalMetaData is cached
	// for other consistencies, record snapshot after whole tables are locked. The recorded meta info is exactly the locked snapshot.
	if conf.Consistency != consistencyTypeLock {
		conn, err := pool.Conn(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		m.recordStartTime(time.Now())
		err = m.recordGlobalMetaData(conn, conf.ServerInfo.ServerType, false)
		if err != nil {
			log.Info("get global metadata failed", zap.Error(err))
		}
		conn.Close()
	}

	connectPool, err := newConnectionsPool(ctx, conf.Threads, pool)
	if err != nil {
		return err
	}
	defer connectPool.Close()

	if conf.PosAfterConnect {
		// record again, to provide a location to exit safe mode for DM
		err = m.recordGlobalMetaData(connectPool.extraConn(), conf.ServerInfo.ServerType, true)
		if err != nil {
			log.Info("get global metadata (after connection pool established) failed", zap.Error(err))
		}
	}

	if conf.Consistency != consistencyTypeLock {
		if err = prepareTableListToDump(conf, connectPool.extraConn()); err != nil {
			return err
		}
	}

	if conf.TransactionalConsistency {
		if conf.Consistency == consistencyTypeFlush || conf.Consistency == consistencyTypeLock {
			log.Info("All the dumping transactions have started. Start to unlock tables")
		}
		if err = conCtrl.TearDown(ctx); err != nil {
			return err
		}
	}

	failpoint.Inject("ConsistencyCheck", nil)

	simpleWriter, err := NewSimpleWriter(conf, d.extStore)
	if err != nil {
		return err
	}
	var writer Writer
	switch strings.ToLower(conf.FileType) {
	case "sql":
		writer = SQLWriter{SimpleWriter: simpleWriter}
	case "csv":
		writer = CSVWriter{SimpleWriter: simpleWriter}
	default:
		return errors.Errorf("unsupported filetype %s", conf.FileType)
	}

	summary.SetLogCollector(summary.NewLogCollector(log.Info))
	summary.SetUnit(summary.BackupUnit)
	defer summary.Summary(summary.BackupUnit)
	if conf.Sql == "" {
		if err = dumpDatabases(ctx, conf, connectPool, writer, func(conn *sql.Conn) (*sql.Conn, error) {
			// make sure that the lock connection is still alive
			err := conCtrl.PingContext(ctx)
			if err != nil {
				return conn, err
			}
			// give up the last broken connection
			conn.Close()
			newConn, err := createConnWithConsistency(ctx, pool)
			if err != nil {
				return conn, err
			}
			conn = newConn
			// renew the master status after connection. dm can't close safe-mode until dm reaches current pos
			if conf.PosAfterConnect {
				err = m.recordGlobalMetaData(conn, conf.ServerInfo.ServerType, true)
				if err != nil {
					return conn, err
				}
			}
			return conn, nil
		}); err != nil {
			return err
		}
	} else {
		if err = dumpSql(ctx, conf, connectPool, writer); err != nil {
			return err
		}
	}

	summary.SetSuccessStatus(true)
	m.recordFinishTime(time.Now())
	return nil
}

func dumpDatabases(pCtx context.Context, conf *Config, connectPool *connectionsPool, writer Writer, rebuildConnFunc func(*sql.Conn) (*sql.Conn, error)) error {
	allTables := conf.Tables
	g, ctx := errgroup.WithContext(pCtx)
	tableDataIRTotal := make([]TableDataIR, 0, len(allTables))
	splitChunkStart := time.Now()
	for dbName, tables := range allTables {
		createDatabaseSQL, err := ShowCreateDatabase(connectPool.extraConn(), dbName)
		if err != nil {
			return err
		}
		if err := writer.WriteDatabaseMeta(ctx, dbName, createDatabaseSQL); err != nil {
			return err
		}

		if len(tables) == 0 {
			continue
		}
		for _, table := range tables {
			table := table
			tableDataIRArray, err := dumpTable(ctx, conf, connectPool.extraConn(), dbName, table, writer)
			if err != nil {
				return err
			}
			tableDataIRTotal = append(tableDataIRTotal, tableDataIRArray...)
		}
	}
	summary.CollectDuration("split chunks", time.Since(splitChunkStart))
	progressPrinter := utils.StartProgress(ctx, "dumpling", int64(len(tableDataIRTotal)), shouldRedirectLog(conf), log.Info)
	defer progressPrinter.Close()
	tableDataStartTime := time.Now()
	for _, tableIR := range tableDataIRTotal {
		tableIR := tableIR
		g.Go(func() error {
			conn := connectPool.getConn()
			defer func() {
				connectPool.releaseConn(conn)
			}()
			retryTime := 0
			var lastErr error
			return utils.WithRetry(ctx, func() (err error) {
				defer func() {
					lastErr = err
					if err == nil {
						progressPrinter.Inc()
					} else {
						errorCount.With(conf.Labels).Inc()
					}
				}()
				retryTime += 1
				log.Debug("trying to dump table chunk", zap.Int("retryTime", retryTime), zap.String("db", tableIR.DatabaseName()),
					zap.String("table", tableIR.TableName()), zap.Int("chunkIndex", tableIR.ChunkIndex()), zap.NamedError("lastError", lastErr))
				if retryTime > 1 {
					conn, err = rebuildConnFunc(conn)
					if err != nil {
						return
					}
				}
				err = tableIR.Start(ctx, conn)
				if err != nil {
					return
				}
				return writer.WriteTableData(ctx, tableIR)
			}, newDumpChunkBackoffer(canRebuildConn(conf.Consistency, conf.TransactionalConsistency)))
		})
	}
	if err := g.Wait(); err != nil {
		summary.CollectFailureUnit("dump", err)
		return err
	} else {
		summary.CollectSuccessUnit("dump cost", len(tableDataIRTotal), time.Since(tableDataStartTime))
	}
	return nil
}

func prepareTableListToDump(conf *Config, pool *sql.Conn) error {
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
	return nil
}

func dumpSql(ctx context.Context, conf *Config, connectPool *connectionsPool, writer Writer) error {
	tableIR, err := SelectFromSql(conf, connectPool.extraConn())
	if err != nil {
		return err
	}

	tableDataStartTime := time.Now()
	err = writer.WriteTableData(ctx, tableIR)
	if err != nil {
		summary.CollectFailureUnit("dump", err)
		return err
	} else {
		summary.CollectSuccessUnit("dump cost", 1, time.Since(tableDataStartTime))
	}
	return nil
}

func dumpTable(ctx context.Context, conf *Config, db *sql.Conn, dbName string, table *TableInfo, writer Writer) ([]TableDataIR, error) {
	tableName := table.Name
	if !conf.NoSchemas {
		if table.Type == TableTypeView {
			viewName := table.Name
			createTableSQL, createViewSQL, err := ShowCreateView(db, dbName, viewName)
			if err != nil {
				return nil, err
			}
			return nil, writer.WriteViewMeta(ctx, dbName, viewName, createTableSQL, createViewSQL)
		}
		createTableSQL, err := ShowCreateTable(db, dbName, tableName)
		if err != nil {
			return nil, err
		}
		if err := writer.WriteTableMeta(ctx, dbName, tableName, createTableSQL); err != nil {
			return nil, err
		}
	}
	// Do not dump table data and return nil
	if conf.NoData {
		return nil, nil
	}

	if conf.Rows != UnspecifiedSize {
		finished, chunksIterArray, err := concurrentDumpTable(ctx, conf, db, dbName, tableName)
		if err != nil || finished {
			return chunksIterArray, err
		}
	}
	tableIR, err := SelectAllFromTable(conf, db, dbName, tableName)
	if err != nil {
		return nil, err
	}

	return []TableDataIR{tableIR}, nil
}

func concurrentDumpTable(ctx context.Context, conf *Config, db *sql.Conn, dbName string, tableName string) (bool, []TableDataIR, error) {
	// try dump table concurrently by split table to chunks
	chunksIterCh := make(chan TableDataIR, defaultDumpThreads)
	errCh := make(chan error, defaultDumpThreads)
	linear := make(chan struct{})

	ctx1, cancel1 := context.WithCancel(ctx)
	defer cancel1()
	var g errgroup.Group
	chunksIterArray := make([]TableDataIR, 0)
	g.Go(func() error {
		splitTableDataIntoChunks(ctx1, chunksIterCh, errCh, linear, dbName, tableName, db, conf)
		return nil
	})

Loop:
	for {
		select {
		case <-ctx.Done():
			return true, chunksIterArray, nil
		case <-linear:
			return false, chunksIterArray, nil
		case chunksIter, ok := <-chunksIterCh:
			if !ok {
				break Loop
			}
			chunksIterArray = append(chunksIterArray, chunksIter)
		case err := <-errCh:
			return false, chunksIterArray, err
		}
	}
	if err := g.Wait(); err != nil {
		return true, chunksIterArray, err
	}
	return true, chunksIterArray, nil
}

func canRebuildConn(consistency string, trxConsistencyOnly bool) bool {
	switch consistency {
	case consistencyTypeLock, consistencyTypeFlush:
		return !trxConsistencyOnly
	case consistencyTypeSnapshot, consistencyTypeNone:
		return true
	default:
		return false
	}
}

func (d *Dumper) Close() error {
	d.cancelCtx()
	return d.dbHandle.Close()
}

func runSteps(d *Dumper, steps ...func(*Dumper) error) error {
	for _, st := range steps {
		err := st(d)
		if err != nil {
			return err
		}
	}
	return nil
}

// createExternalStore is an initialization step of Dumper.
func createExternalStore(d *Dumper) error {
	ctx, conf := d.ctx, d.conf
	b, err := storage.ParseBackend(conf.OutputDirPath, &conf.BackendOptions)
	if err != nil {
		return err
	}
	extStore, err := storage.Create(ctx, b, false)
	if err != nil {
		return err
	}
	d.extStore = extStore
	return nil
}

// startHttpService is an initialization step of Dumper.
func startHttpService(d *Dumper) error {
	conf := d.conf
	if conf.StatusAddr != "" {
		go func() {
			err := startDumplingService(conf.StatusAddr)
			if err != nil {
				log.Error("dumpling stops to serving service", zap.Error(err))
			}
		}()
	}
	return nil
}

// openSQLDB is an initialization step of Dumper.
func openSQLDB(d *Dumper) error {
	conf := d.conf
	pool, err := sql.Open("mysql", conf.GetDSN(""))
	if err != nil {
		return errors.Trace(err)
	}
	d.dbHandle = pool
	return nil
}

// detectServerInfo is an initialization step of Dumper.
func detectServerInfo(d *Dumper) error {
	db, conf := d.dbHandle, d.conf
	versionStr, err := SelectVersion(db)
	if err != nil {
		conf.ServerInfo = ServerInfoUnknown
		return err
	}
	conf.ServerInfo = ParseServerInfo(versionStr)
	return nil
}

// resolveAutoConsistency is an initialization step of Dumper.
func resolveAutoConsistency(d *Dumper) error {
	conf := d.conf
	if conf.Consistency != "auto" {
		return nil
	}
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		conf.Consistency = "snapshot"
	case ServerTypeMySQL, ServerTypeMariaDB:
		conf.Consistency = "flush"
	default:
		conf.Consistency = "none"
	}
	return nil
}

// tidbSetPDClientForGC is an initialization step of Dumper.
func tidbSetPDClientForGC(d *Dumper) error {
	ctx, si, pool := d.ctx, d.conf.ServerInfo, d.dbHandle
	if si.ServerType != ServerTypeTiDB ||
		si.ServerVersion == nil ||
		si.ServerVersion.Compare(*gcSafePointVersion) < 0 {
		return nil
	}
	pdAddrs, err := GetPdAddrs(pool)
	if err != nil {
		return err
	}
	if len(pdAddrs) > 0 {
		doPdGC, err := checkSameCluster(ctx, pool, pdAddrs)
		if err != nil {
			log.Warn("meet error while check whether fetched pd addr and TiDB belongs to one cluster", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
		} else if doPdGC {
			pdClient, err := pd.NewClientWithContext(ctx, pdAddrs, pd.SecurityOption{})
			if err != nil {
				log.Warn("create pd client to control GC failed", zap.Error(err), zap.Strings("pdAddrs", pdAddrs))
			}
			d.tidbPDClientForGC = pdClient
		}
	}
	return nil
}

// tidbGetSnapshot is an initialization step of Dumper.
func tidbGetSnapshot(d *Dumper) error {
	conf, doPdGC := d.conf, d.tidbPDClientForGC != nil
	consistency := conf.Consistency
	pool, ctx := d.dbHandle, d.ctx
	if conf.Snapshot == "" && (doPdGC || consistency == "snapshot") {
		conn, err := pool.Conn(ctx)
		if err != nil {
			log.Warn("cannot get snapshot from TiDB", zap.Error(err))
			return nil
		}
		snapshot, err := getSnapshot(conn)
		_ = conn.Close()
		if err != nil {
			log.Warn("cannot get snapshot from TiDB", zap.Error(err))
			return nil
		}
		conf.Snapshot = snapshot
		return nil
	}
	return nil
}

// tidbStartGCSavepointUpdateService is an initialization step of Dumper.
func tidbStartGCSavepointUpdateService(d *Dumper) error {
	ctx, pool, conf := d.ctx, d.dbHandle, d.conf
	snapshot, si := conf.Snapshot, conf.ServerInfo
	if d.tidbPDClientForGC != nil {
		snapshotTS, err := parseSnapshotToTSO(pool, snapshot)
		if err != nil {
			return err
		}
		go updateServiceSafePoint(ctx, d.tidbPDClientForGC, defaultDumpGCSafePointTTL, snapshotTS)
	} else if si.ServerType == ServerTypeTiDB {
		log.Warn("If the amount of data to dump is large, criteria: (data more than 60GB or dumped time more than 10 minutes)\n" +
			"you'd better adjust the tikv_gc_life_time to avoid export failure due to TiDB GC during the dump process.\n" +
			"Before dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '720h' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n" +
			"After dumping: run sql `update mysql.tidb set VARIABLE_VALUE = '10m' where VARIABLE_NAME = 'tikv_gc_life_time';` in tidb.\n")
	}
	return nil
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

// setSessionParam is an initialization step of Dumper.
func setSessionParam(d *Dumper) error {
	conf, pool := d.conf, d.dbHandle
	si := conf.ServerInfo
	consistency, snapshot := conf.Consistency, conf.Snapshot
	sessionParam := conf.SessionParams
	if si.ServerType == ServerTypeTiDB {
		sessionParam[TiDBMemQuotaQueryName] = conf.TiDBMemQuotaQuery
	}
	if snapshot != "" {
		if si.ServerType != ServerTypeTiDB {
			return errors.New("snapshot consistency is not supported for this server")
		}
		if consistency == consistencyTypeSnapshot {
			hasTiKV, err := CheckTiDBWithTiKV(pool)
			if err != nil {
				return err
			}
			if hasTiKV {
				sessionParam["tidb_snapshot"] = snapshot
			}
		}
	}
	if newPool, err := resetDBWithSessionParams(pool, conf.GetDSN(""), conf.SessionParams); err != nil {
		return errors.Trace(err)
	} else {
		d.dbHandle = newPool
	}
	return nil
}

func shouldRedirectLog(conf *Config) bool {
	return conf.Logger != nil || conf.LogFile != ""
}
