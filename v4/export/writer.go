package export

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/br/pkg/utils"
	"strings"
	"text/template"

	"github.com/pingcap/br/pkg/storage"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

type Writer struct {
	cfg        *Config
	cntPool    *connectionsPool
	fileFmt    FileFormat
	extStorage storage.ExternalStorage

	rebuildConnFn   func(*sql.Conn) (*sql.Conn, error)
	receivedIRCount int
}

func NewWriter(config *Config, pool *connectionsPool, externalStore storage.ExternalStorage) *Writer {
	sw := &Writer{
		cfg:        config,
		cntPool:    pool,
		extStorage: externalStore,
	}
	switch strings.ToLower(config.FileType) {
	case "sql":
		sw.fileFmt = FileFormatSQLText
	case "csv":
		sw.fileFmt = FileFormatCSV
	}
	return sw
}

func (w *Writer) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	conf := w.cfg
	fileName, err := (&outputFileNamer{DB: db}).render(conf.OutputFileTemplate, outputFileTemplateSchema)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createSQL, w.extStorage, fileName+".sql", conf.CompressType)
}

func (w *Writer) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	conf := w.cfg
	fileName, err := (&outputFileNamer{DB: db, Table: table}).render(conf.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createSQL, w.extStorage, fileName+".sql", conf.CompressType)
}

func (w *Writer) WriteViewMeta(ctx context.Context, db, view, createTableSQL, createViewSQL string) error {
	conf := w.cfg
	fileNameTable, err := (&outputFileNamer{DB: db, Table: view}).render(conf.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	fileNameView, err := (&outputFileNamer{DB: db, Table: view}).render(conf.OutputFileTemplate, outputFileTemplateView)
	if err != nil {
		return err
	}
	err = writeMetaToFile(ctx, db, createTableSQL, w.extStorage, fileNameTable+".sql", conf.CompressType)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createViewSQL, w.extStorage, fileNameView+".sql", conf.CompressType)
}

func (w *Writer) WriteTableData(ctx context.Context, meta TableMeta, irStream <-chan TableDataIR) error {
	if irStream == nil {
		return nil
	}
	log.Debug("start dumping table...",
		zap.String("table", meta.TableName()),
		zap.Stringer("format", w.fileFmt))
	chunkIndex := 0
	channelClosed := false
	for !channelClosed {
		select {
		case <-ctx.Done():
			log.Info("context has been done",
				zap.String("table", meta.TableName()),
				zap.Stringer("format", w.fileFmt))
			return nil
		case ir, ok := <-irStream:
			if !ok {
				channelClosed = true
				break
			}
			w.receivedIRCount++
			conn := w.cntPool.getConn()
			err := w.startTableIRQueryWithRetry(ctx, conn, meta, ir, chunkIndex)
			if err != nil {
				w.cntPool.releaseConn(conn)
				return err
			}
			chunkIndex++
			w.cntPool.releaseConn(conn)
		}
	}
	log.Debug("dumping table successfully",
		zap.String("table", meta.TableName()))
	return nil
}

func (w *Writer) startTableIRQueryWithRetry(ctx context.Context, conn *sql.Conn,
	meta TableMeta, ir TableDataIR, chunkIndex int) error {
	conf := w.cfg
	retryTime := 0
	var lastErr error
	return utils.WithRetry(ctx, func() (err error) {
		defer func() {
			lastErr = err
			if err != nil {
				errorCount.With(conf.Labels).Inc()
			}
		}()
		retryTime += 1
		log.Debug("trying to dump table chunk", zap.Int("retryTime", retryTime), zap.String("db", meta.DatabaseName()),
			zap.String("table", meta.TableName()), zap.Int("chunkIndex", chunkIndex), zap.NamedError("lastError", lastErr))
		if retryTime > 1 {
			conn, err = w.rebuildConnFn(conn)
			if err != nil {
				return
			}
		}
		err = ir.Start(ctx, conn)
		if err != nil {
			return
		}
		defer ir.Close()
		return w.writeTableData(ctx, meta, ir, chunkIndex)
	}, newDumpChunkBackoffer(canRebuildConn(conf.Consistency, conf.TransactionalConsistency)))
}

func (w *Writer) writeTableData(ctx context.Context, meta TableMeta, ir TableDataIR, curChkIdx int) error {
	conf, format := w.cfg, w.fileFmt
	namer := newOutputFileNamer(meta, curChkIdx, conf.Rows != UnspecifiedSize, conf.FileSize != UnspecifiedSize)
	fileName, err := namer.NextName(conf.OutputFileTemplate, w.fileFmt.Extension())
	if err != nil {
		return err
	}

	for {
		fileWriter, tearDown := buildInterceptFileWriter(w.extStorage, fileName, conf.CompressType)
		err = format.WriteInsert(ctx, conf, meta, ir, fileWriter)
		tearDown(ctx)
		if err != nil {
			return err
		}

		if w, ok := fileWriter.(*InterceptFileWriter); ok && !w.SomethingIsWritten {
			break
		}

		if conf.FileSize == UnspecifiedSize {
			break
		}
		fileName, err = namer.NextName(conf.OutputFileTemplate, w.fileFmt.Extension())
		if err != nil {
			return err
		}
	}
	return nil
}

func writeMetaToFile(ctx context.Context, target, metaSQL string, s storage.ExternalStorage, path string, compressType storage.CompressType) error {
	fileWriter, tearDown, err := buildFileWriter(ctx, s, path, compressType)
	if err != nil {
		return err
	}
	defer tearDown(ctx)

	return WriteMeta(ctx, &metaData{
		target:  target,
		metaSQL: metaSQL,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, fileWriter)
}

type outputFileNamer struct {
	ChunkIndex int
	FileIndex  int
	DB         string
	Table      string
	format     string
}

type csvOption struct {
	nullValue string
	separator []byte
	delimiter []byte
}

func newOutputFileNamer(meta TableMeta, chunkIdx int, rows, fileSize bool) *outputFileNamer {
	o := &outputFileNamer{
		DB:    meta.DatabaseName(),
		Table: meta.TableName(),
	}
	o.ChunkIndex = chunkIdx
	o.FileIndex = 0
	if rows && fileSize {
		o.format = "%09d%04d"
	} else if fileSize {
		o.format = "%09[2]d"
	} else {
		o.format = "%09[1]d"
	}
	return o
}

func (namer *outputFileNamer) render(tmpl *template.Template, subName string) (string, error) {
	var bf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&bf, subName, namer); err != nil {
		return "", err
	}
	return bf.String(), nil
}

func (namer *outputFileNamer) Index() string {
	return fmt.Sprintf(namer.format, namer.ChunkIndex, namer.FileIndex)
}

func (namer *outputFileNamer) NextName(tmpl *template.Template, fileType string) (string, error) {
	res, err := namer.render(tmpl, outputFileTemplateData)
	namer.FileIndex++
	return res + "." + fileType, err
}
