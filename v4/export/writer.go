package export

import (
	"bytes"
	"context"
	"text/template"

	"github.com/pingcap/br/pkg/storage"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
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

type SQLWriter struct{ SimpleWriter }

func (f SQLWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	log.Debug("start dumping table...", zap.String("table", ir.TableName()))

	// just let `database.table.sql` be `database.table.0.sql`
	/*if fileName == "" {
		// set initial file name
		fileName = fmt.Sprintf("%s.%s.sql", ir.DatabaseName(), ir.TableName())
		if f.cfg.FileSize != UnspecifiedSize {
			fileName = fmt.Sprintf("%s.%s.%d.sql", ir.DatabaseName(), ir.TableName(), 0)
		}
	}*/
	namer := newOutputFileNamer(ir)
	fileName, err := namer.NextName(f.cfg.OutputFileTemplate)
	if err != nil {
		return err
	}
	fileName += ".sql"
	chunksIter := ir
	defer chunksIter.Rows().Close()

	for {
		fileWriter, tearDown := buildInterceptFileWriter(f.cfg.ExternalStorage, fileName)
		err = WriteInsert(ctx, chunksIter, fileWriter, f.cfg.FileSize, f.cfg.StatementSize)
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
		fileName, err = namer.NextName(f.cfg.OutputFileTemplate)
		if err != nil {
			return err
		}
		fileName += ".sql"
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
	}, fileWriter)
}

type CSVWriter struct{ SimpleWriter }

type outputFileNamer struct {
	Index int
	DB    string
	Table string
}

type csvOption struct {
	nullValue string
	separator []byte
	delimiter []byte
}

func newOutputFileNamer(ir TableDataIR) *outputFileNamer {
	return &outputFileNamer{
		Index: ir.ChunkIndex(),
		DB:    ir.DatabaseName(),
		Table: ir.TableName(),
	}
}

func (namer *outputFileNamer) render(tmpl *template.Template, subName string) (string, error) {
	var bf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&bf, subName, namer); err != nil {
		return "", err
	}
	return bf.String(), nil
}

func (namer *outputFileNamer) NextName(tmpl *template.Template) (string, error) {
	res, err := namer.render(tmpl, outputFileTemplateData)
	namer.Index++
	return res, err
}

func (f CSVWriter) WriteTableData(ctx context.Context, ir TableDataIR) error {
	log.Debug("start dumping table in csv format...", zap.String("table", ir.TableName()))

	namer := newOutputFileNamer(ir)
	fileName, err := namer.NextName(f.cfg.OutputFileTemplate)
	if err != nil {
		return err
	}
	fileName += ".csv"
	chunksIter := ir
	defer chunksIter.Rows().Close()

	opt := &csvOption{
		nullValue: f.cfg.CsvNullValue,
		separator: []byte(f.cfg.CsvSeparator),
		delimiter: []byte(f.cfg.CsvDelimiter),
	}

	for {
		fileWriter, tearDown := buildInterceptFileWriter(f.cfg.ExternalStorage, fileName)
		err = WriteInsertInCsv(ctx, chunksIter, fileWriter, f.cfg.NoHeader, opt, f.cfg.FileSize)
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
		fileName, err = namer.NextName(f.cfg.OutputFileTemplate)
		if err != nil {
			return err
		}
		fileName += ".csv"
	}
	log.Debug("dumping table in csv format successfully",
		zap.String("table", ir.TableName()))
	return nil
}
