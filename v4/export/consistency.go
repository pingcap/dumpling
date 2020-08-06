package export

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func NewConsistencyController(ctx context.Context, conf *Config, session *sql.DB) (ConsistencyController, error) {
	resolveAutoConsistency(conf)
	conn, err := session.Conn(ctx)
	if err != nil {
		return nil, err
	}
	switch conf.Consistency {
	case "flush":
		return &ConsistencyFlushTableWithReadLock{
			ctx:        ctx,
			serverType: conf.ServerInfo.ServerType,
			conn:       conn,
		}, nil
	case "lock":
		return &ConsistencyLockDumpingTables{
			ctx:       ctx,
			conn:      conn,
			allTables: conf.Tables,
		}, nil
	case "snapshot":
		if conf.ServerInfo.ServerType != ServerTypeTiDB {
			return nil, withStack(errors.New("snapshot consistency is not supported for this server"))
		}
		return &ConsistencyNone{}, nil
	case "none":
		return &ConsistencyNone{}, nil
	default:
		return nil, withStack(fmt.Errorf("invalid consistency option %s", conf.Consistency))
	}
}

type ConsistencyController interface {
	Setup() error
	TearDown() error
}

type ConsistencyNone struct{}

func (c *ConsistencyNone) Setup() error {
	return nil
}

func (c *ConsistencyNone) TearDown() error {
	return nil
}

type ConsistencyFlushTableWithReadLock struct {
	ctx        context.Context
	serverType ServerType
	conn       *sql.Conn
}

func (c *ConsistencyFlushTableWithReadLock) Setup() error {
	if c.serverType == ServerTypeTiDB {
		return withStack(errors.New("'flush table with read lock' cannot be used to ensure the consistency in TiDB"))
	}
	return FlushTableWithReadLock(c.ctx, c.conn)
}

func (c *ConsistencyFlushTableWithReadLock) TearDown() error {
	defer c.conn.Close()
	return UnlockTables(c.ctx, c.conn)
}

type ConsistencyLockDumpingTables struct {
	ctx       context.Context
	conn      *sql.Conn
	allTables DatabaseTables
}

func (c *ConsistencyLockDumpingTables) Setup() error {
	for dbName, tables := range c.allTables {
		for _, table := range tables {
			err := LockTables(c.ctx, c.conn, dbName, table.Name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *ConsistencyLockDumpingTables) TearDown() error {
	defer c.conn.Close()
	return UnlockTables(c.ctx, c.conn)
}

const showMasterStatusFieldNum = 5
const snapshotFieldIndex = 1

func resolveAutoConsistency(conf *Config) {
	if conf.Consistency != "auto" {
		return
	}
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		conf.Consistency = "snapshot"
	case ServerTypeMySQL, ServerTypeMariaDB:
		conf.Consistency = "flush"
	default:
		conf.Consistency = "none"
	}
}
