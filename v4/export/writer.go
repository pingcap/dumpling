package export

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteTableData(ctx context.Context, ir TableDataIR) error
}

type DummyWriter struct {
	cfg *Config
}

func NewDummyWriter(config *Config) Writer {
	return &DummyWriter{cfg: config}
}

func (f *DummyWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s-schema-create.sql", db))
	fsStringWriter := NewFileSystemWriter(fileName, false)
	meta := &metaData{
		target:  db,
		metaSQL: createSQL,
	}
	return WriteMeta(meta, fsStringWriter, f.cfg)
}

func (f *DummyWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s.%s-schema.sql", db, table))
	fsStringWriter := NewFileSystemWriter(fileName, false)
	meta := &metaData{
		target:  table,
		metaSQL: createSQL,
	}
	return WriteMeta(meta, fsStringWriter, f.cfg)
}

func (f *DummyWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s.%s.sql", ir.DatabaseName(), ir.TableName()))
	fsStringWriter := NewFileSystemWriter(fileName, true)

	return WriteInsert(ir, fsStringWriter, f.cfg)
}

type FileSystemWriter struct {
	path string

	file *os.File
	once sync.Once
	err  error
}

func (w *FileSystemWriter) initFileHandle() {
	w.file, w.err = os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY, 0755)
}

func (w *FileSystemWriter) WriteString(str string) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.once.Do(w.initFileHandle)
	return w.file.WriteString(str)
}

func NewFileSystemWriter(path string, lazyHandleCreation bool) *FileSystemWriter {
	w := &FileSystemWriter{path: path}
	if !lazyHandleCreation {
		w.once.Do(w.initFileHandle)
	}
	return w
}

// SizedWriter controls the string size output to each down-stream writer. When the limit is reached,
// it will use the result of `changeDownStream(cnt)` to replace the current down-stream writer.
// The argument `cnt` here means the number of used down-stream writers.
type SizedWriter struct {
	limitSize uint // bytes

	currentSize      uint // bytes
	downStreamCounts uint

	changeDownStream func(cnt uint) io.StringWriter
	downStream       io.StringWriter
}

func NewSizedWriter(limitSizeInBytes uint, changeDownStream func(uint) io.StringWriter) *SizedWriter {
	return &SizedWriter{
		limitSize:        limitSizeInBytes,
		changeDownStream: changeDownStream,
	}
}

func (s *SizedWriter) WriteString(str string) (int, error) {
	strLen := uint(len(str))
	if s.downStream == nil || s.currentSize+strLen > s.limitSize {
		s.downStream = s.changeDownStream(s.downStreamCounts)
		s.downStreamCounts += 1
		s.currentSize = 0
	}
	s.currentSize += strLen
	return s.downStream.WriteString(str)
}
