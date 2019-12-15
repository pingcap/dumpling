package dumpling

import (
	"context"
	"fmt"
	"path"

	"github.com/pingcap/dumpling/v4/export"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteTableData(ctx context.Context, ir export.TableDataIR) error
}

type FileSystemWriter struct {
	cfg *export.Config
}

func NewFileSystemWriter(config *export.Config) Writer {
	return &FileSystemWriter{cfg: config}
}

func (f *FileSystemWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s-schema-create.sql", db))
	fsStringWriter := export.NewFileSystemWriter(fileName, false)
	meta := &metaData{
		target:  db,
		metaSQL: createSQL,
	}
	var err error
	export.WriteMeta(meta, fsStringWriter, f.cfg, func(e error) {
		err = withStack(e)
	})
	return err
}

func (f *FileSystemWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s.%s-schema.sql", db, table))
	fsStringWriter := export.NewFileSystemWriter(fileName, false)
	meta := &metaData{
		target:  table,
		metaSQL: createSQL,
	}
	var err error
	export.WriteMeta(meta, fsStringWriter, f.cfg, func(e error) {
		err = withStack(e)
	})
	return err
}

func (f *FileSystemWriter) WriteTableData(ctx context.Context, ir export.TableDataIR) error {
	fileName := path.Join(f.cfg.OutputDirPath, fmt.Sprintf("%s.%s.sql", ir.DatabaseName(), ir.TableName()))
	fsStringWriter := export.NewFileSystemWriter(fileName, false)

	var err error
	export.WriteInsert(ir, fsStringWriter, f.cfg, func(e error) {
		err = withStack(e)
	})
	return err
}
