// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

// Writer is the interface that supports writing database/table/view's meta info and table's data info.
type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteViewMeta(ctx context.Context, db, table, createTableSQL, createViewSQL string) error
	WriteTableData(ctx context.Context, ir TableDataIR) error
}

// SimpleWriter is an implement of Writer which supports simple writer functions
type SimpleWriter struct {
	cfg      *Config
	extStore storage.ExternalStorage
}

// NewSimpleWriter returns a new SimpleWriter
func NewSimpleWriter(config *Config, externalStore storage.ExternalStorage) (SimpleWriter, error) {
	sw := SimpleWriter{
		cfg:      config,
		extStore: externalStore,
	}
	return sw, nil
}

// WriteDatabaseMeta implements Writer.WriteDatabaseMeta
func (f SimpleWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName, err := (&outputFileNamer{DB: db}).render(f.cfg.OutputFileTemplate, outputFileTemplateSchema)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createSQL, f.extStore, fileName+".sql", f.cfg.CompressType)
}

// WriteTableMeta implements Writer.WriteTableMeta
func (f SimpleWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName, err := (&outputFileNamer{DB: db, Table: table}).render(f.cfg.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createSQL, f.extStore, fileName+".sql", f.cfg.CompressType)
}

// WriteViewMeta implements Writer.WriteViewMeta
func (f SimpleWriter) WriteViewMeta(ctx context.Context, db, view, createTableSQL, createViewSQL string) error {
	fileNameTable, err := (&outputFileNamer{DB: db, Table: view}).render(f.cfg.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	fileNameView, err := (&outputFileNamer{DB: db, Table: view}).render(f.cfg.OutputFileTemplate, outputFileTemplateView)
	if err != nil {
		return err
	}
	err = writeMetaToFile(ctx, db, createTableSQL, f.extStore, fileNameTable+".sql", f.cfg.CompressType)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createViewSQL, f.extStore, fileNameView+".sql", f.cfg.CompressType)
}

// WriteTableData implements Writer.WriteTableData
func (f SimpleWriter) WriteTableData(ctx context.Context, ir TableDataIR) (err error) {
	log.Debug("start dumping table...", zap.String("table", ir.TableName()))

	defer ir.Close()
	namer := newOutputFileNamer(ir, f.cfg.Rows != UnspecifiedSize, f.cfg.FileSize != UnspecifiedSize)
	fileType := strings.ToLower(f.cfg.FileType)
	fileName, err := namer.NextName(f.cfg.OutputFileTemplate, fileType)
	if err != nil {
		return err
	}

	for {
		fileWriter, tearDown := buildInterceptFileWriter(f.extStore, fileName, f.cfg.CompressType)
		switch fileType {
		case "sql":
			err = WriteInsert(ctx, ir, fileWriter, f.cfg)
		case "csv":
			err = WriteInsertInCsv(ctx, ir, fileWriter, f.cfg)
		}
		tearDown(ctx)
		if err != nil {
			return err
		}

		if w, ok := fileWriter.(*InterceptFileWriter); ok && !w.SomethingIsWritten {
			break
		}

		if f.cfg.FileSize == UnspecifiedSize {
			break
		}
		fileName, err = namer.NextName(f.cfg.OutputFileTemplate, fileType)
		if err != nil {
			return err
		}
	}
	log.Debug("dumping table successfully",
		zap.String("table", ir.TableName()))
	return nil
}

func writeMetaToFile(ctx context.Context, target, metaSQL string, s storage.ExternalStorage, path string, compressType storage.CompressType) error {
	fileWriter, tearDown, err := buildFileWriter(ctx, s, path, compressType)
	if err != nil {
		return errors.Trace(err)
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

func newOutputFileNamer(ir TableDataIR, rows, fileSize bool) *outputFileNamer {
	o := &outputFileNamer{
		DB:    ir.DatabaseName(),
		Table: ir.TableName(),
	}
	o.ChunkIndex = ir.ChunkIndex()
	o.FileIndex = 0
	switch {
	case rows && fileSize:
		o.format = "%09d%04d"
	case fileSize:
		o.format = "%09[2]d"
	default:
		o.format = "%09[1]d"
	}
	return o
}

func (namer *outputFileNamer) render(tmpl *template.Template, subName string) (string, error) {
	var bf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&bf, subName, namer); err != nil {
		return "", errors.Trace(err)
	}
	return bf.String(), nil
}

func (namer *outputFileNamer) Index() string {
	return fmt.Sprintf(namer.format, namer.ChunkIndex, namer.FileIndex)
}

func (namer *outputFileNamer) NextName(tmpl *template.Template, fileType string) (string, error) {
	res, err := namer.render(tmpl, outputFileTemplateData)
	namer.FileIndex++
	return res + "." + fileType, errors.Trace(err)
}
