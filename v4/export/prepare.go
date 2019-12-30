package export

import (
	"database/sql"
	"errors"
	"strings"
)

func detectServerInfo(db *sql.DB) (ServerInfo, error) {
	versionStr, err := SelectVersion(db)
	if err != nil {
		return ServerInfoUnknown, err
	}
	return ParseServerInfo(versionStr), nil
}

func prepareDumpingDatabases(conf *Config, db *sql.DB) ([]string, error) {
	if conf.Database == "" {
		return ShowDatabases(db)
	} else {
		return strings.Split(conf.Database, ","), nil
	}
}

func listAllTables(db *sql.DB, databaseNames []string) (DatabaseTables, error) {
	dbTables := DatabaseTables{}
	for _, dbName := range databaseNames {
		err := UseDatabase(db, dbName)
		if err != nil {
			return nil, err
		}
		tables, err := ShowTables(db)
		if err != nil {
			return nil, err
		}
		dbTables[dbName] = tables
	}
	return dbTables, nil
}

type Consistency int8

const (
	ConsistencyNone Consistency = iota
	ConsistencyFlushTableWithReadLock
	ConsistencyLockDumpingTables
	ConsistencySnapshot
)

type ConsistencyController struct {
	strategy   Consistency
	serverType ServerType
	allTables  map[databaseName][]tableName
	dsn        string
	snapshot   string
	db         *sql.DB
}

func (c *ConsistencyController) Setup(conf *Config, strategy Consistency) error {
	c.strategy = strategy
	c.serverType = conf.ServerInfo.ServerType
	c.allTables = conf.Tables
	c.dsn = conf.getDSN("")
	c.snapshot = conf.Snapshot
	switch strategy {
	case ConsistencyNone:
		return nil
	case ConsistencyFlushTableWithReadLock:
		return c.setupFlushTableWithReadLock()
	case ConsistencyLockDumpingTables:
		return c.setupTableLock()
	case ConsistencySnapshot:
		return c.setupSnapshot()
	default:
		panic("unsupported consistency strategy")
	}
	return nil
}

func (c *ConsistencyController) TearDown() error {
	switch c.strategy {
	case ConsistencyNone:
		return nil
	case ConsistencyFlushTableWithReadLock:
		return c.tearDownFlushTableWithReadLock()
	case ConsistencyLockDumpingTables:
		return c.tearDownTableLock()
	case ConsistencySnapshot:
		return c.tearDownSnapshot()
	default:
		panic("unsupported consistency strategy")
	}
	return nil
}

func (c *ConsistencyController) setupFlushTableWithReadLock() error {
	if c.serverType == ServerTypeTiDB {
		return withStack(errors.New("'flush table with read lock' cannot be used to ensure the consistency in TiDB"))
	}
	var err error
	c.db, err = sql.Open("mysql", c.dsn)
	if err != nil {
		return err
	}
	return FlushTableWithReadLock(c.db)
}

func (c *ConsistencyController) tearDownFlushTableWithReadLock() error {
	if c.db == nil {
		return withStack(errors.New("ConsistencyController lost database connection"))
	}
	defer c.db.Close()
	return UnlockTables(c.db)
}

func (c *ConsistencyController) setupTableLock() error {
	var err error
	c.db, err = sql.Open("mysql", c.dsn)
	if err != nil {
		return err
	}
	for dbName, tables := range c.allTables {
		for _, table := range tables {
			err := LockTables(c.db, dbName, table)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ConsistencyController) tearDownTableLock() error {
	if c.db == nil {
		return withStack(errors.New("ConsistencyController lost database connection"))
	}
	defer c.db.Close()
	return UnlockTables(c.db)
}

const showMasterStatusFieldNum = 5
const snapshotFieldIndex = 1

func (c *ConsistencyController) setupSnapshot() error {
	if c.serverType != ServerTypeTiDB {
		return withStack(errors.New("snapshot consistency is not supported for this server"))
	}
	if c.snapshot == "" {
		str, err := ShowMasterStatus(c.db, showMasterStatusFieldNum)
		if err != nil {
			return err
		}
		c.snapshot = str[snapshotFieldIndex]
	}
	return SetTiDBSnapshot(c.db, c.snapshot)
}

func (c *ConsistencyController) tearDownSnapshot() error {
	return c.db.Close()
}
