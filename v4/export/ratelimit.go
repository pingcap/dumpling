package export

import (
	"context"
	"database/sql"
)

type rateLimit struct {
	token chan *sql.Conn
}

func newRateLimit(ctx context.Context, n int, pool *sql.DB) (*rateLimit, error) {
	rl := &rateLimit{
		token: make(chan *sql.Conn, n),
	}
	for i := 0; i < n; i++ {
		tx, err := createConnWithConsistency(ctx, pool)
		if err != nil {
			return nil, err
		}
		rl.putToken(tx)
	}
	return rl, nil
}

func (r *rateLimit) getToken() *sql.Conn {
	return <-r.token
}

func (r *rateLimit) putToken(conn *sql.Conn) {
	select {
	case r.token <- conn:
	default:
		panic("put a redundant token")
	}
}

func (r *rateLimit) Close() {
	for i := 0; i < cap(r.token); i++ {
		conn := r.getToken()
		conn.Close()
	}
}
