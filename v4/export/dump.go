package export

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/pingcap/dumpling/v4/log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
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
		validateSpecifiedSQL,
		validateFileFormat)
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

	writer := NewWriter(conf, connectPool, d.extStore)
	writer.rebuildConnFn = func(conn *sql.Conn) (*sql.Conn, error) {
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
	}

	summary.SetLogCollector(summary.NewLogCollector(log.Info))
	summary.SetUnit(summary.BackupUnit)
	defer summary.Summary(summary.BackupUnit)
	if conf.Sql == "" {
		if err = d.dumpDatabases(connectPool.extraConn(), writer); err != nil {
			return err
		}
	} else {
		if err = d.dumpSql(connectPool.extraConn(), writer); err != nil {
			return err
		}
	}

	summary.SetSuccessStatus(true)
	m.recordFinishTime(time.Now())
	return nil
}

func (d *Dumper) dumpDatabases(conn *sql.Conn, writer *Writer) error {
	conf := d.conf
	allTables := conf.Tables
	ctx, cancel := context.WithCancel(d.ctx)
	defer cancel()
	var writingGroup errgroup.Group
	tableDataStartTime := time.Now()
	for dbName, tables := range allTables {
		createDatabaseSQL, err := ShowCreateDatabase(conn, dbName)
		if err != nil {
			return err
		}
		err = writer.WriteDatabaseMeta(ctx, dbName, createDatabaseSQL)
		if err != nil {
			return err
		}

		if len(tables) == 0 {
			continue
		}
		for _, table := range tables {
			meta, err := dumpTableMeta(conf, conn, dbName, table)
			if err != nil {
				return err
			}

			if table.Type == TableTypeView {
				err = writer.WriteViewMeta(ctx, dbName, table.Name, meta.ShowCreateTable(), meta.ShowCreateView())
			} else {
				err = writer.WriteTableMeta(ctx, dbName, table.Name, meta.ShowCreateTable())
			}
			if err != nil {
				return err
			}

			tableIRStream := make(chan TableDataIR, defaultDumpThreads)
			err = d.dumpTableData(conn, meta, tableIRStream)
			if err != nil {
				return err
			}
			writingGroup.Go(func() error {
				return writer.WriteTableData(ctx, meta, tableIRStream)
			})
		}
	}
	if err := writingGroup.Wait(); err != nil {
		summary.CollectFailureUnit("dump table data", err)
		return err
	}
	summary.CollectSuccessUnit("dump cost", writer.receivedIRCount, time.Since(tableDataStartTime))
	return nil
}

func (d *Dumper) dumpTableData(conn *sql.Conn, meta TableMeta, ir chan<- TableDataIR) error {
	conf := d.conf
	defer close(ir)
	if conf.NoData {
		return nil
	}
	if conf.Rows == UnspecifiedSize {
		return d.sequentialDumpTable(conn, meta, ir)
	}
	return d.concurrentDumpTable(conn, meta, ir)
}

func (d *Dumper) sequentialDumpTable(conn *sql.Conn, meta TableMeta, ir chan<- TableDataIR) error {
	conf := d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()
	tableIR, err := SelectAllFromTable(conf, conn, db, tbl)
	if err != nil {
		return err
	}
	ir <- tableIR
	return nil
}

func (d *Dumper) concurrentDumpTable(conn *sql.Conn, meta TableMeta, ir chan<- TableDataIR) error {
	ctx, conf := d.ctx, d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()
	if conf.ServerInfo.ServerType == ServerTypeTiDB &&
		conf.ServerInfo.ServerVersion != nil &&
		conf.ServerInfo.ServerVersion.Compare(*tableSampleVersion) >= 0 {
		log.Debug("dumping TiDB tables with TABLESAMPLE",
			zap.String("database", db), zap.String("table", tbl))
		return d.concurrentDumpTiDBTables(conn, meta, ir)
	}
	field, err := pickupPossibleField(db, tbl, conn, conf)
	if err != nil {
		return nil
	}
	if field == "" {
		// skip split chunk logic if not found proper field
		log.Debug("skip concurrent dump due to no proper field", zap.String("field", field))
		return d.sequentialDumpTable(conn, meta, ir)
	}

	min, max, err := d.selectMinAndMaxIntValue(conn, db, tbl, field)
	if err != nil {
		return err
	}
	log.Debug("get int bounding values",
		zap.String("lower", min.String()),
		zap.String("upper", max.String()))

	count := estimateCount(db, tbl, conn, field, conf)
	log.Info("get estimated rows count", zap.Uint64("estimateCount", count))
	if count < conf.Rows {
		// skip chunk logic if estimates are low
		log.Debug("skip concurrent dump due to estimate count < rows",
			zap.Uint64("estimate count", count),
			zap.Uint64("conf.rows", conf.Rows),
		)
		return d.sequentialDumpTable(conn, meta, ir)
	}

	// every chunk would have eventual adjustments
	estimatedChunks := count / conf.Rows
	estimatedStep := new(big.Int).Sub(max, min).Uint64()/estimatedChunks + 1
	bigEstimatedStep := new(big.Int).SetUint64(estimatedStep)
	cutoff := new(big.Int).Set(min)

	selectField, selectLen, err := buildSelectField(conn, db, tbl, conf.CompleteInsert)
	if err != nil {
		return err
	}
	if selectField == "" {
		selectField = "''"
	}

	orderByClause, err := buildOrderByClause(conf, conn, db, tbl)
	if err != nil {
		return err
	}

	nullValueCondition := fmt.Sprintf("`%s` IS NULL OR ", escapeString(field))
	for max.Cmp(cutoff) >= 0 {
		nextCutOff := new(big.Int).Add(cutoff, bigEstimatedStep)
		where := fmt.Sprintf("%s(`%s` >= %d AND `%s` < %d)", nullValueCondition, escapeString(field), cutoff, escapeString(field), nextCutOff)
		query := buildSelectQuery(db, tbl, selectField, buildWhereCondition(conf, where), orderByClause)
		if len(nullValueCondition) > 0 {
			nullValueCondition = ""
		}
		select {
		case <-ctx.Done():
			return nil
		case ir <- newTableData(query, selectLen):
		}
		cutoff = nextCutOff
	}
	return nil
}

var z = &big.Int{}

func (d *Dumper) selectMinAndMaxIntValue(conn *sql.Conn, db, tbl, field string) (*big.Int, *big.Int, error) {
	ctx, conf := d.ctx, d.conf
	query := fmt.Sprintf("SELECT MIN(`%s`),MAX(`%s`) FROM `%s`.`%s`",
		escapeString(field), escapeString(field), escapeString(db), escapeString(tbl))
	if conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, conf.Where)
	}
	log.Debug("split chunks", zap.String("query", query))

	var smin sql.NullString
	var smax sql.NullString
	row := conn.QueryRowContext(ctx, query)
	err := row.Scan(&smin, &smax)
	if err != nil {
		log.Error("split chunks - get max min failed", zap.String("query", query), zap.Error(err))
		return z, z, err
	}
	if !smax.Valid || !smin.Valid {
		// found no data
		log.Warn("no data to dump", zap.String("schema", db), zap.String("table", tbl))
		return z, z, nil
	}

	max := new(big.Int)
	min := new(big.Int)
	var ok bool
	if max, ok = max.SetString(smax.String, 10); !ok {
		return z, z, errors.Errorf("fail to convert max value %s in query %s", smax.String, query)
	}
	if min, ok = min.SetString(smin.String, 10); !ok {
		return z, z, errors.Errorf("fail to convert min value %s in query %s", smin.String, query)
	}
	return min, max, nil
}

func (d *Dumper) concurrentDumpTiDBTables(conn *sql.Conn, meta TableMeta, ir chan<- TableDataIR) error {
	ctx, conf := d.ctx, d.conf
	db, tbl := meta.DatabaseName(), meta.TableName()

	pkNames, pkVals, err := selectTiDBTableSample(db, tbl, conn, meta)

	selectField, selectLen, err := buildSelectField(conn, db, tbl, conf.CompleteInsert)
	if err != nil {
		return err
	}
	if selectField == "" {
		selectField = "''"
	}

	where := make([]string, 0, len(pkVals)+1)
	where = append(where, fmt.Sprintf("(%s) < %s", pkNames, pkVals[0]))
	for i := 1; i < len(pkVals); i++ {
		low, up := pkVals[i-1], pkVals[i]
		where = append(where, fmt.Sprintf("(%s) < (%s) AND (%s) >= (%s)", pkNames, low, pkNames, up))
	}
	where = append(where, fmt.Sprintf("(%s) >= (%s)", pkNames, pkVals[len(pkVals)-1]))

	var orderByClause string
	if pkNames == "_tidb_rowid" {
		orderByClause = "ORDER BY `_tidb_rowid`"
	} else {
		orderByClause = fmt.Sprintf("ORDER BY %s", pkNames)
	}

	for _, w := range where {
		query := buildSelectQuery(db, tbl, selectField, buildWhereCondition(conf, w), orderByClause)
		contextDone := false
		select {
		case <-ctx.Done():
			contextDone = true
		case ir <- newTableData(query, selectLen):
		}
		if contextDone {
			break
		}
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

func dumpTableMeta(conf *Config, conn *sql.Conn, db string, table *TableInfo) (TableMeta, error) {
	tbl := table.Name
	selectField, _, err := buildSelectField(conn, db, tbl, conf.CompleteInsert)
	if err != nil {
		return nil, err
	}

	var colTypes []*sql.ColumnType
	// If all columns are generated
	if selectField == "" {
		colTypes, err = GetColumnTypes(conn, "*", db, tbl)
	} else {
		colTypes, err = GetColumnTypes(conn, selectField, db, tbl)
	}

	meta := &tableMeta{
		database:      db,
		table:         tbl,
		colTypes:      colTypes,
		selectedField: selectField,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}

	if conf.NoSchemas {
		return meta, nil
	}
	if table.Type == TableTypeView {
		viewName := table.Name
		createTableSQL, createViewSQL, err := ShowCreateView(conn, db, viewName)
		if err != nil {
			return meta, err
		}
		meta.showCreateTable = createTableSQL
		meta.showCreateView = createViewSQL
		return meta, nil
	}
	createTableSQL, err := ShowCreateTable(conn, db, tbl)
	meta.showCreateTable = createTableSQL
	return meta, nil
}

func (d *Dumper) dumpSql(conn *sql.Conn, writer *Writer) error {
	ctx, conf := d.ctx, d.conf
	meta, tableIR, err := SelectFromSql(conf, conn)
	if err != nil {
		return err
	}

	tableDataStartTime := time.Now()
	err = writer.WriteTableData(ctx, meta, makeOneTimeChan(tableIR))
	if err != nil {
		summary.CollectFailureUnit("dump", err)
		return err
	} else {
		summary.CollectSuccessUnit("dump cost", 1, time.Since(tableDataStartTime))
	}
	return nil
}

func makeOneTimeChan(ir TableDataIR) <-chan TableDataIR {
	oneTimeChan := make(chan TableDataIR, 1)
	oneTimeChan <- ir
	close(oneTimeChan)
	return oneTimeChan
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
