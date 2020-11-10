package export

import (
	"context"
	"database/sql"
	"github.com/lichunzhu/go-mysql/client"
	"strconv"
)

type connectionsPool struct {
	conns        chan *sql.Conn
	createdConns []*sql.Conn
}
type connectionsPoolNew struct {
	conns        chan *client.Conn
	createdConns []*client.Conn
}

func (conn *connectionsPoolNew) Close() error {
	var err error
	for _, conn := range conn.createdConns {
		err2 := conn.Close()
		if err2 != nil {
			err = err2
		}
	}
	return err
}

func (conn *connectionsPoolNew) releaseConn(c *client.Conn) {
	select {
	case conn.conns <- c:
	default:
		panic("put a redundant conn")
	}
}

func (r *connectionsPoolNew) getConn() *client.Conn {
	return <-r.conns
}

func newConnectionsPool(ctx context.Context, n int, pool *sql.DB) (*connectionsPool, error) {
	connectPool := &connectionsPool{
		conns:        make(chan *sql.Conn, n),
		createdConns: make([]*sql.Conn, 0, n),
	}
	for i := 0; i < n; i++ {
		conn, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			connectPool.Close()
			return connectPool, err
		}
		connectPool.releaseConn(conn)
		connectPool.createdConns = append(connectPool.createdConns, conn)
	}
	return connectPool, nil
}

func newConnectionsPoolNew(ctx context.Context, n int, conf *Config) (*connectionsPoolNew, error) {
	connectPool := &connectionsPoolNew{
		conns:        make(chan *client.Conn, n),
		createdConns: make([]*client.Conn, 0, n),
	}
	for i := 0; i < n; i++ {
		conn, _ := client.Connect(conf.Host+":"+strconv.Itoa(conf.Port), conf.User, conf.Password, "")

		query := "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */"
		conn.Execute(query)

		query = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
		conn.Execute(query)
		//connectPool.releaseConn(conn)

		connectPool.createdConns = append(connectPool.createdConns, conn)
		connectPool.conns <- conn
	}
	return connectPool, nil
}

func (r *connectionsPool) getConn() *sql.Conn {
	return <-r.conns
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
