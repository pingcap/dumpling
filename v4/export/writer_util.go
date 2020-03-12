package export

import (
	"bufio"
	"fmt"
	"github.com/pingcap/dumpling/v4/log"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
	"sync"
)

const lengthLimit = 1048576

type Dao struct {
	bp      sync.Pool
}

func NewDao() (d *Dao) {
	d = &Dao{
		bp: sync.Pool{
			New: func() interface{} {
				return &strings.Builder{}
			},
		},
	}
	return
}

type writeManager struct {
	sb *strings.Builder
	writer io.StringWriter
	lengthLimit int

	err error
}

func newWriteManager(sb *strings.Builder, writer io.StringWriter, lengthLimit int) *writeManager {
	return &writeManager{
		sb:          sb,
		writer:      writer,
		lengthLimit: lengthLimit,
		err:         nil,
	}
}

func (w writeManager) flushSb() error {
	if w.sb.Len() == 0 {
		return nil
	}
	err := write(w.writer, w.sb.String())
	w.sb.Reset()
	w.sb.Grow(lengthLimit)
	if err != nil {
		return err
	}
	return nil
}

func (w writeManager) WriteString(s string) {
	if w.err != nil {
		return
	}
	if len(s) >= w.lengthLimit {
		w.err = w.flushSb()
		if w.err != nil {
			return
		}
		w.err = write(w.writer, s)
		return
	} else if w.sb.Len() + len(s) >= w.lengthLimit {
		w.err = w.flushSb()
		if w.err != nil {
			return
		}
	}
	w.sb.WriteString(s)
}

func (w writeManager) WriteByte(s byte) {
	if w.err != nil {
		return
	}
	if w.sb.Len() + 1 >= w.lengthLimit {
		w.err = w.flushSb()
		if w.err != nil {
			return
		}
	}
	w.sb.WriteByte(s)
}

func WriteMeta(meta MetaIR, w io.StringWriter) error {
	log.Zap().Debug("start dumping meta data", zap.String("target", meta.TargetName()))

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(w, fmt.Sprintf("%s;\n", meta.MetaSQL())); err != nil {
		return err
	}

	log.Zap().Debug("finish dumping meta data", zap.String("target", meta.TargetName()))
	return nil
}

func WriteInsert(tblIR TableDataIR, w io.StringWriter) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	var err error

	dao := NewDao()
	sb := dao.bp.Get().(*strings.Builder)
	sb.Grow(lengthLimit)
	wm := newWriteManager(sb, w, lengthLimit)
	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		wm.WriteString(specCmtIter.Next())
		wm.WriteString("\n")
	}

	var (
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s VALUES\n", wrapBackTicks(tblIR.TableName()))
		row                   = MakeRowReceiver(tblIR.ColumnTypes())
		counter               = 0
	)

	for fileRowIter.HasNextSQLRowIter() {
		wm.WriteString(insertStatementPrefix)

		fileRowIter = fileRowIter.NextSQLRowIter()
		for fileRowIter.HasNext() {
			if err = fileRowIter.Next(row); err != nil {
				log.Zap().Error("scanning from sql.Row failed", zap.Error(err))
				return err
			}

			row.WriteToStringBuilder(wm)
			counter += 1

			var splitter string
			if fileRowIter.HasNext() {
				splitter = ","
			} else {
				splitter = ";"
			}
			wm.WriteString(splitter)
			wm.WriteString("\n")

			if wm.err != nil {
				return err
			}
		}
	}

	log.Zap().Debug("dumping table",
		zap.String("table", tblIR.TableName()),
		zap.Int("record counts", counter))
	err = wm.flushSb()
	dao.bp.Put(sb)
	return err
}

func write(writer io.StringWriter, str string) error {
	_, err := writer.WriteString(str)
	if err != nil {
		log.Zap().Error("writing failed",
			zap.String("string", str),
			zap.Error(err))
	}
	return err
}

func buildFileWriter(path string) (io.StringWriter, func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		log.Zap().Error("open file failed",
			zap.String("path", path),
			zap.Error(err))
		return nil, nil, err
	}
	log.Zap().Debug("opened file", zap.String("path", path))
	buf := bufio.NewWriter(file)
	tearDownRoutine := func() {
		_ = buf.Flush()
		err := file.Close()
		if err == nil {
			return
		}
		log.Zap().Error("close file failed",
			zap.String("path", path),
			zap.Error(err))
	}
	return buf, tearDownRoutine, nil
}

func buildLazyFileWriter(path string) (io.StringWriter, func()) {
	var file *os.File
	var buf *bufio.Writer
	lazyStringWriter := &LazyStringWriter{}
	initRoutine := func() error {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		file = f
		if err != nil {
			log.Zap().Error("open file failed",
				zap.String("path", path),
				zap.Error(err))
		}
		log.Zap().Debug("opened file", zap.String("path", path))
		buf = bufio.NewWriter(file)
		lazyStringWriter.StringWriter = buf
		return err
	}
	lazyStringWriter.initRoutine = initRoutine

	tearDownRoutine := func() {
		if file == nil {
			return
		}
		log.Zap().Debug("tear down lazy file writer...")
		_ = buf.Flush()
		err := file.Close()
		if err == nil {
			return
		}
		log.Zap().Error("close file failed", zap.String("path", path))
	}
	return lazyStringWriter, tearDownRoutine
}

type LazyStringWriter struct {
	initRoutine func() error
	sync.Once
	io.StringWriter
	err error
}

func (l *LazyStringWriter) WriteString(str string) (int, error) {
	l.Do(func() { l.err = l.initRoutine() })
	if l.err != nil {
		return 0, fmt.Errorf("open file error: %s", l.err.Error())
	}
	return l.StringWriter.WriteString(str)
}

// InterceptStringWriter is an interceptor of io.StringWriter,
// tracking whether a StringWriter has written something.
type InterceptStringWriter struct {
	io.StringWriter
	SomethingIsWritten bool
}

func (w *InterceptStringWriter) WriteString(str string) (int, error) {
	if len(str) > 0 {
		w.SomethingIsWritten = true
	}
	return w.StringWriter.WriteString(str)
}

func wrapBackTicks(identifier string) string {
	if !strings.HasPrefix(identifier, "`") && !strings.HasSuffix(identifier, "`") {
		return wrapStringWith(identifier, "`")
	}
	return identifier
}

func wrapStringWith(str string, wrapper string) string {
	return fmt.Sprintf("%s%s%s", wrapper, str, wrapper)
}
