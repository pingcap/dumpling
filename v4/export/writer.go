package export

import (
	"context"
	"fmt"
	"os"
	"path"

	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

type Writer interface {
	WriteDatabaseMeta(ctx context.Context, db, createSQL string) error
	WriteTableMeta(ctx context.Context, db, table, createSQL string) error
	WriteTableData(ctx context.Context, dbName, tableName, fileName string, chunksIter *tableDataChunks) error
}

type SimpleWriter struct {
	cfg *Config
}

func NewSimpleWriter(config *Config) (Writer, error) {
	sw := &SimpleWriter{cfg: config}
	return sw, os.MkdirAll(config.OutputDirPath, 0755)
}

func (f *SimpleWriter) WriteDatabaseMeta(ctx context.Context, db, createSQL string) error {
	fileName := fmt.Sprintf("%s-schema-create.sql", db)
	filePath := path.Join(f.cfg.OutputDirPath, fileName)
	return writeMetaToFile(db, createSQL, filePath)
}

func (f *SimpleWriter) WriteTableMeta(ctx context.Context, db, table, createSQL string) error {
	fileName := fmt.Sprintf("%s.%s-schema.sql", db, table)
	filePath := path.Join(f.cfg.OutputDirPath, fileName)
	return writeMetaToFile(db, createSQL, filePath)
}

func (f *SimpleWriter) WriteTableData(ctx context.Context, dbName, tableName, fileName string, chunksIter *tableDataChunks) error {
	defer chunksIter.Rows().Close()
	log.Zap().Debug("start dumping table...", zap.String("table", tableName))

	if fileName == "" {
		fileName = fmt.Sprintf("%s.%s.sql", dbName, tableName)
		if f.cfg.FileSize != UnspecifiedSize {
			fileName = fmt.Sprintf("%s.%s.%d.sql", dbName, tableName, 0)
		}
	}

	chunkCount := 0
	for {
		filePath := path.Join(f.cfg.OutputDirPath, fileName)
		fileWriter, tearDown := buildLazyFileWriter(filePath)
		intWriter := &InterceptStringWriter{StringWriter: fileWriter}
		err := WriteInsert(chunksIter, intWriter)
		tearDown()
		if err != nil {
			return err
		}

		if !intWriter.SomethingIsWritten {
			break
		}

		if f.cfg.FileSize == UnspecifiedSize {
			break
		}
		chunkCount += 1
		fileName = fmt.Sprintf("%s.%s.%d.sql", dbName, tableName, chunkCount)
	}
	log.Zap().Debug("dumping table successfully",
		zap.String("table", tableName))
	return nil
}

func writeMetaToFile(target, metaSQL, path string) error {
	fileWriter, tearDown, err := buildFileWriter(path)
	if err != nil {
		return err
	}
	defer tearDown()

	return WriteMeta(&metaData{
		target:  target,
		metaSQL: metaSQL,
	}, fileWriter)
}
