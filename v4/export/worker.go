package export

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// WorkerPool contains a pool of workers
type WorkerPool struct {
	limit   uint
	workers chan *Worker
	name    string
}

// Worker identified by ID
type Worker struct {
	ID uint64
}

type taskFunc func()

// NewWorkerPool returns a WorkPool
func NewWorkerPool(limit uint, name string) *WorkerPool {
	workers := make(chan *Worker, limit)
	for i := uint(0); i < limit; i++ {
		workers <- &Worker{ID: uint64(i + 1)}
	}
	return &WorkerPool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

// Apply executes a task
func (pool *WorkerPool) Apply(fn taskFunc) {
	var worker *Worker
	select {
	case worker = <-pool.workers:
	default:
		log.Debug("wait for workers", zap.String("pool", pool.name))
		worker = <-pool.workers
	}
	go func() {
		fn()
		pool.recycle(worker)
	}()
}

func (pool *WorkerPool) recycle(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
}

// HasWorker checks if the pool has unallocated workers
func (pool *WorkerPool) HasWorker() bool {
	return len(pool.workers) > 0
}
