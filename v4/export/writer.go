package export

import (
	"io"
	"os"
	"sync"
)

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
