package export

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/pingcap/br/pkg/storage"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteViewMeta(ctx context.Context, db, table, createTableSQL, createViewSQL string) error
	WriteTableData(ctx context.Context, ir TableDataIR) error
}

type SimpleWriter struct {
	cfg *Config
}

func NewSimpleWriter(config *Config) (SimpleWriter, error) {
	sw := SimpleWriter{cfg: config}
	return sw, nil
}

func (f SimpleWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName, err := (&outputFileNamer{DB: db}).render(f.cfg.OutputFileTemplate, outputFileTemplateSchema)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createSQL, f.cfg.ExternalStorage, fileName+".sql")
}

func (f SimpleWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName, err := (&outputFileNamer{DB: db, Table: table}).render(f.cfg.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createSQL, f.cfg.ExternalStorage, fileName+".sql")
}

func (f SimpleWriter) WriteViewMeta(ctx context.Context, db, view, createTableSQL, createViewSQL string) error {
	fileNameTable, err := (&outputFileNamer{DB: db, Table: view}).render(f.cfg.OutputFileTemplate, outputFileTemplateTable)
	if err != nil {
		return err
	}
	fileNameView, err := (&outputFileNamer{DB: db, Table: view}).render(f.cfg.OutputFileTemplate, outputFileTemplateView)
	if err != nil {
		return err
	}
	err = writeMetaToFile(ctx, db, createTableSQL, f.cfg.ExternalStorage, fileNameTable+".sql")
	if err != nil {
		return err
	}
	return writeMetaToFile(ctx, db, createViewSQL, f.cfg.ExternalStorage, fileNameView+".sql")
}

type SQLWriter struct{ SimpleWriter }

func (f SQLWriter) WriteTableData(ctx context.Context, ir TableDataIR) (err error) {
	log.Debug("start dumping table...", zap.String("table", ir.TableName()))

	defer ir.Close()
	namer := newOutputFileNamer(ir, f.cfg.Rows != UnspecifiedSize, f.cfg.FileSize != UnspecifiedSize)
	fileType := strings.ToLower(f.cfg.FileType)
	fileName, err := namer.NextName(f.cfg.OutputFileTemplate, fileType)
	if err != nil {
		return err
	}

	for {
		fileWriter, tearDown := buildInterceptFileWriter(f.cfg.ExternalStorage, fileName)
		err = WriteInsert(ctx, ir, fileWriter, f.cfg.FileSize, f.cfg.StatementSize)
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

func writeMetaToFile(ctx context.Context, target, metaSQL string, s storage.ExternalStorage, path string) error {
	fileWriter, tearDown, err := buildFileWriter(ctx, s, path)
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

type CSVWriter struct{ SimpleWriter }

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

func (f CSVWriter) WriteTableData(ctx context.Context, ir TableDataIR) (err error) {
	log.Debug("start dumping table in csv format...", zap.String("table", ir.TableName()))

	defer ir.Close()
	namer := newOutputFileNamer(ir, f.cfg.Rows != UnspecifiedSize, f.cfg.FileSize != UnspecifiedSize)
	fileType := strings.ToLower(f.cfg.FileType)
	fileName, err := namer.NextName(f.cfg.OutputFileTemplate, fileType)
	if err != nil {
		return err
	}

	opt := &csvOption{
		nullValue: f.cfg.CsvNullValue,
		separator: []byte(f.cfg.CsvSeparator),
		delimiter: []byte(f.cfg.CsvDelimiter),
	}

	for {
		fileWriter, tearDown := buildInterceptFileWriter(f.cfg.ExternalStorage, fileName)
		err = WriteInsertInCsv(ctx, ir, fileWriter, f.cfg.NoHeader, opt, f.cfg.FileSize)
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
	log.Debug("dumping table in csv format successfully",
		zap.String("table", ir.TableName()))
	return nil
}
