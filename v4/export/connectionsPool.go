// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package export

import (
	"context"
	"database/sql"
)

type connectionsPool struct {
	conns        chan *sql.Conn
	createdConns []*sql.Conn
}

func newConnectionsPool(ctx context.Context, n int, pool *sql.DB) (*connectionsPool, error) {
	connectPool := &connectionsPool{
		conns:        make(chan *sql.Conn, n),
		createdConns: make([]*sql.Conn, 0, n+1),
	}
	for i := 0; i < n+1; i++ {
		conn, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			connectPool.Close()
			return connectPool, err
		}
		if i != n {
			connectPool.releaseConn(conn)
		}
		connectPool.createdConns = append(connectPool.createdConns, conn)
	}
	return connectPool, nil
}

func (r *connectionsPool) getConn() *sql.Conn {
	return <-r.conns
}

func (r *connectionsPool) extraConn() *sql.Conn {
	return r.createdConns[len(r.createdConns)-1]
}

func (r *connectionsPool) Close() error {
	var err error
	for _, conn := range r.createdConns {
		err2 := conn.Close()
		if err2 != nil {
			err = err2
		}
	}
	return err
}

func (r *connectionsPool) releaseConn(conn *sql.Conn) {
	select {
	case r.conns <- conn:
	default:
		panic("put a redundant conn")
	}
}
