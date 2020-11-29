package export

import (
	"bytes"
	"context"
	_ "database/sql"
	"fmt"
	"io"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/storage"

	"github.com/pingcap/dumpling/v4/log"
)

const lengthLimit = 1048576

// TODO make this configurable, 5 mb is a good minimum size but on low latency/high bandwidth network you can go a lot bigger
const hardcodedS3ChunkSize = 5 * 1024 * 1024

var pool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}

type bufferPool struct {
	bp *sync.Pool
}

func newBufferPool(colTypes []string) *bufferPool {
	return &bufferPool{
		bp: &sync.Pool{
			New: func() interface{} {
				b := MakeRowReceiverArr(colTypes)
				return b
			},
		},
	}
}

func (bpool *bufferPool) Get() *RowReceiverArr {
	return bpool.bp.Get().(*RowReceiverArr)
}

func (bpool *bufferPool) Put(rr *RowReceiverArr) {
	//TODO1: 这里可能导致想插入null值，缺插入空字符串
	for _, v := range rr.receivers {
		switch v.(type) {
		case *SQLTypeString:
			v.(*SQLTypeString).RawBytes = v.(*SQLTypeString).RawBytes[:0]
		case *SQLTypeBytes:
			v.(*SQLTypeBytes).RawBytes = v.(*SQLTypeBytes).RawBytes[:0]
		case *SQLTypeNumber:
			v.(*SQLTypeNumber).RawBytes = v.(*SQLTypeNumber).RawBytes[:0]
		}
	}
	bpool.bp.Put(rr)
}

type writerPipe struct {
	input  chan *bytes.Buffer
	closed chan struct{}
	errCh  chan error

	currentFileSize      uint64
	currentStatementSize uint64

	fileSizeLimit      uint64
	statementSizeLimit uint64

	w storage.Writer
}

func newWriterPipe(w storage.Writer, fileSizeLimit, statementSizeLimit uint64) *writerPipe {
	return &writerPipe{
		input:  make(chan *bytes.Buffer, 8),
		closed: make(chan struct{}),
		errCh:  make(chan error, 1),
		w:      w,

		currentFileSize:      0,
		currentStatementSize: 0,
		fileSizeLimit:        fileSizeLimit,
		statementSizeLimit:   statementSizeLimit,
	}
}

func (b *writerPipe) Run(ctx context.Context) {
	defer close(b.closed)
	var errOccurs bool
	for {
		select {
		case s, ok := <-b.input:
			if !ok {
				return
			}
			if errOccurs {
				continue
			}
			err := writeBytes(ctx, b.w, s.Bytes())
			s.Reset()
			pool.Put(s)
			if err != nil {
				errOccurs = true
				b.errCh <- err
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *writerPipe) AddFileSize(fileSize uint64) {
	b.currentFileSize += fileSize
	b.currentStatementSize += fileSize
}

func (b *writerPipe) Error() error {
	select {
	case err := <-b.errCh:
		return err
	default:
		return nil
	}
}

func (b *writerPipe) ShouldSwitchFile() bool {
	return b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit
}

func (b *writerPipe) ShouldSwitchStatement() bool {
	return (b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit) ||
		(b.statementSizeLimit != UnspecifiedSize && b.currentStatementSize >= b.statementSizeLimit)
}

func WriteMeta(ctx context.Context, meta MetaIR, w storage.Writer) error {
	log.Debug("start dumping meta data", zap.String("target", meta.TargetName()))

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(ctx, w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(ctx, w, meta.MetaSQL()); err != nil {
		return err
	}

	log.Debug("finish dumping meta data", zap.String("target", meta.TargetName()))
	return nil
}

func WriteInsert(pCtx context.Context, tblIR TableDataIR, w storage.Writer, fileSizeLimit, statementSizeLimit uint64) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, fileSizeLimit, statementSizeLimit)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		bf.WriteString(specCmtIter.Next())
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	var (
		insertStatementPrefix string
		counter               = 0
		escapeBackSlash       = tblIR.EscapeBackSlash()
		err                   error
	)

	selectedField := tblIR.SelectedField()
	// if has generated column
	if selectedField != "" {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s %s VALUES\n",
			wrapBackTicks(escapeString(tblIR.TableName())), selectedField)
	} else {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s VALUES\n",
			wrapBackTicks(escapeString(tblIR.TableName())))
	}
	insertStatementPrefixLen := uint64(len(insertStatementPrefix))
	wp.currentStatementSize = 0
	var wg1 sync.WaitGroup
	wg1.Add(1)
	shouldSwitchFile := struct {
		sync.Mutex
		flag bool
	}{flag: false}
	rowsChan := make(chan *RowReceiverArr, 200)

	colTypes := tblIR.ColumnTypes()
	//rowPool := sync.Pool{New: func() interface{} {
	//	return MakeRowReceiverArr(colTypes)
	//}}
	rowPool2 := newBufferPool(colTypes)

	rowReceiverClone := func(colTypes []string, r *RowReceiverArr) *RowReceiverArr {
		//rowReceiverArr := rowPool.Get().(*RowReceiverArr).receivers
		rowReceiverArr := rowPool2.Get().receivers
		//rowReceiverArr := MakeRowReceiverArr(colTypes).receivers

		for i, v := range r.receivers {
			switch v.(type) {
			case *SQLTypeString:
				rowReceiverArr[i].(*SQLTypeString).Assign(v.(*SQLTypeString).RawBytes)
			case *SQLTypeBytes:
				rowReceiverArr[i].(*SQLTypeBytes).Assign(r.receivers[i].(*SQLTypeBytes).RawBytes)
			case *SQLTypeNumber:
				rowReceiverArr[i].(*SQLTypeNumber).Assign(r.receivers[i].(*SQLTypeNumber).RawBytes)
			}
		}
		return &RowReceiverArr{
			bound:     false,
			receivers: rowReceiverArr,
		}
	}

	go func() {
		isHead := false
		bf.WriteString(insertStatementPrefix)
		wp.AddFileSize(insertStatementPrefixLen)
		defer wg1.Done()

		for {
			i, ok := <-rowsChan
			if !ok {
				bf.Truncate(bf.Len() - 2)
				bf.WriteString(";\n")
				break
			}
			if isHead {
				wp.currentStatementSize = 0
				bf.WriteString(insertStatementPrefix)
				wp.AddFileSize(insertStatementPrefixLen)
				isHead = false
			}
			lastBfSize := bf.Len()
			i.WriteToBuffer(bf, escapeBackSlash)
			rowPool2.Put(i)
			wp.AddFileSize(uint64(bf.Len()-lastBfSize) + 2) // 2 is for ",\n" and ";\n"
			shouldSwitch := wp.ShouldSwitchStatement()
			if !shouldSwitch {
				bf.WriteString(",\n")
			} else {
				bf.WriteString(";\n")
				isHead = true
			}
			if wp.ShouldSwitchFile() {
				// TODO2: 对于switch的判断，如果用读写并发方式，似乎无法保证读与写改判断的一致,文件可能会比预期多一些数据
				shouldSwitchFile.Lock()
				shouldSwitchFile.flag = true
				shouldSwitchFile.Unlock()
			}

			if bf.Len() >= lengthLimit {
				select {
				case <-pCtx.Done():
					return
				case _ = <-wp.errCh:
					return
				case wp.input <- bf:
					bf = pool.Get().(*bytes.Buffer)
					if bfCap := bf.Cap(); bfCap < lengthLimit {
						bf.Grow(lengthLimit - bfCap)
					}
				}
			}

		}
	}()
	row0 := MakeRowReceiverArr(colTypes)
	for fileRowIter.HasNext() {
		if err = fileRowIter.Decode(row0); err != nil {
			log.Error("scanning from sql.Row failed", zap.Error(err))
			return err
		}
		row := rowReceiverClone(colTypes, row0)
		rowsChan <- row

		shouldSwitchFile.Lock()
		if shouldSwitchFile.flag {
			shouldSwitchFile.Unlock()
			break
		}
		shouldSwitchFile.Unlock()
		fileRowIter.Next()
	}

	close(rowsChan)
	wg1.Wait()

	log.Debug("dumping table",
		zap.String("table", tblIR.TableName()),
		zap.Int("record counts", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	if err = fileRowIter.Error(); err != nil {
		return err
	}
	return wp.Error()
}

func WriteInsertInCsv(pCtx context.Context, tblIR TableDataIR, w storage.Writer, noHeader bool, opt *csvOption, fileSizeLimit uint64) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, fileSizeLimit, UnspecifiedSize)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	var (
		row             = MakeRowReceiver(tblIR.ColumnTypes())
		counter         = 0
		escapeBackSlash = tblIR.EscapeBackSlash()
		err             error
	)

	if !noHeader && len(tblIR.ColumnNames()) != 0 {
		for i, col := range tblIR.ColumnNames() {
			bf.Write(opt.delimiter)
			escape([]byte(col), bf, getEscapeQuotation(escapeBackSlash, opt.delimiter))
			bf.Write(opt.delimiter)
			if i != len(tblIR.ColumnTypes())-1 {
				bf.Write(opt.separator)
			}
		}
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	for fileRowIter.HasNext() {
		if err = fileRowIter.Decode(row); err != nil {
			log.Error("scanning from sql.Row failed", zap.Error(err))
			return err
		}

		lastBfSize := bf.Len()
		row.WriteToBufferInCsv(bf, escapeBackSlash, opt)
		counter += 1
		wp.currentFileSize += uint64(bf.Len()-lastBfSize) + 1 // 1 is for "\n"

		bf.WriteByte('\n')
		if bf.Len() >= lengthLimit {
			select {
			case <-pCtx.Done():
				return pCtx.Err()
			case err := <-wp.errCh:
				return err
			case wp.input <- bf:
				bf = pool.Get().(*bytes.Buffer)
				if bfCap := bf.Cap(); bfCap < lengthLimit {
					bf.Grow(lengthLimit - bfCap)
				}
			}
		}

		fileRowIter.Next()
		if wp.ShouldSwitchFile() {
			break
		}
	}

	log.Debug("dumping table",
		zap.String("table", tblIR.TableName()),
		zap.Int("record counts", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	if err = fileRowIter.Error(); err != nil {
		return err
	}
	return wp.Error()
}

func write(ctx context.Context, writer storage.Writer, str string) error {
	_, err := writer.Write(ctx, []byte(str))
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(str)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.String("string", str[:outputLength]),
			zap.Error(err))
	}
	return err
}

func writeBytes(ctx context.Context, writer storage.Writer, p []byte) error {
	_, err := writer.Write(ctx, p)
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(p)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.ByteString("string", p[:outputLength]),
			zap.String("writer", fmt.Sprintf("%#v", writer)),
			zap.Error(err))
	}
	return err
}

func buildFileWriter(ctx context.Context, s storage.ExternalStorage, path string) (storage.Writer, func(ctx context.Context), error) {
	fullPath := s.URI() + path
	uploader, err := s.CreateUploader(ctx, path)
	if err != nil {
		log.Error("open file failed",
			zap.String("path", fullPath),
			zap.Error(err))
		return nil, nil, err
	}
	writer := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize)
	log.Debug("opened file", zap.String("path", fullPath))
	tearDownRoutine := func(ctx context.Context) {
		err := writer.Close(ctx)
		if err == nil {
			return
		}
		log.Error("close file failed",
			zap.String("path", fullPath),
			zap.Error(err))
	}
	return writer, tearDownRoutine, nil
}

func buildInterceptFileWriter(s storage.ExternalStorage, path string) (storage.Writer, func(context.Context)) {
	var writer storage.Writer
	fullPath := s.URI() + path
	fileWriter := &InterceptFileWriter{}
	initRoutine := func(ctx context.Context) error {
		uploader, err := s.CreateUploader(ctx, path)
		if err != nil {
			log.Error("open file failed",
				zap.String("path", fullPath),
				zap.Error(err))
			return err
		}
		w := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize)
		writer = w
		log.Debug("opened file", zap.String("path", fullPath))
		fileWriter.Writer = writer
		return err
	}
	fileWriter.initRoutine = initRoutine

	tearDownRoutine := func(ctx context.Context) {
		if writer == nil {
			return
		}
		log.Debug("tear down lazy file writer...")
		err := writer.Close(ctx)
		if err != nil {
			log.Error("close file failed", zap.String("path", fullPath))
		}
	}
	return fileWriter, tearDownRoutine
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

// InterceptFileWriter is an interceptor of os.File,
// tracking whether a StringWriter has written something.
type InterceptFileWriter struct {
	storage.Writer
	sync.Once
	initRoutine func(context.Context) error
	err         error

	SomethingIsWritten bool
}

func (w *InterceptFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	w.Do(func() { w.err = w.initRoutine(ctx) })
	if len(p) > 0 {
		w.SomethingIsWritten = true
	}
	if w.err != nil {
		return 0, fmt.Errorf("open file error: %s", w.err.Error())
	}
	return w.Writer.Write(ctx, p)
}

func (w *InterceptFileWriter) Close(ctx context.Context) error {
	return w.Writer.Close(ctx)
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
