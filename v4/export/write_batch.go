package export

import (
	"context"
	"io"
)

const lengthLimit = 500 * (1 << 20)

type writeBatch struct {
	ctx context.Context
	input  chan string
	writer io.StringWriter
	
	closed chan struct{}
	errCh  chan error
}

func NewWriteBatch(ctx context.Context, writer io.StringWriter) *writeBatch {
	return &writeBatch{
		ctx:    ctx,
		input:  make(chan string, 500 * 1024),
		writer: writer,
		closed: make(chan struct{}),
		errCh:  make(chan error, 1),
	}
}

func (w writeBatch) Write(s string) {
	select {
	case w.input <- s:
	case <-w.ctx.Done():
	}
}

func (w writeBatch) Closed() chan struct{} {
	return w.closed
}

func (w writeBatch) Error() chan error {
	return w.errCh
}

func (w writeBatch) Run() {
	defer func() {
		close(w.closed)
		close(w.errCh)
	}()
	var err error
	for {
		select {
		case s, ok := <-w.input:
			if !ok {
				return
			}
			err = write(w.writer, s)
			if err != nil {
				w.errCh <- err
				return
			}
		case <-w.ctx.Done():
			return
		}
	}
}
