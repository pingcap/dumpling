package export

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
)

type dumplingRow []sql.NullString

func (d dumplingRow) BindAddress(args []interface{}) {
	for i := range d {
		args[i] = &d[i]
	}
}

func WriteMeta(meta MetaIR, w io.StringWriter, cfg *Config) error {
	log := cfg.Logger
	log.Debug("start dumping meta data for target %s", meta.TargetName())

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(w, fmt.Sprintf("%s\n", specCmtIter.Next()), log); err != nil {
			return err
		}
	}

	if err := write(w, fmt.Sprintf("%s\n", meta.MetaSQL()), log); err != nil {
		return err
	}

	log.Debug("finish dumping meta data for target %s", meta.TargetName())
	return nil
}

func WriteInsert(tblIR TableDataIR, w io.StringWriter, cfg *Config) error {
	log := cfg.Logger
	rowIter := tblIR.Rows()
	if !rowIter.HasNext() {
		return nil
	}

	log.Debug("start dumping for table %s", tblIR.TableName())
	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(w, fmt.Sprintf("%s\n", specCmtIter.Next()), log); err != nil {
			return err
		}
	}

	tblName := wrapBackticks(tblIR.TableName())
	if err := write(w, fmt.Sprintf("INSERT INTO %s VALUES \n", tblName), log); err != nil {
		return err
	}

	for rowIter.HasNext() {
		var dumplingRow = make(dumplingRow, tblIR.ColumnCount())
		if err := rowIter.Next(dumplingRow); err != nil {
			log.Error("scanning from sql.Row failed, error: %s", err.Error())
			return err
		}

		row := handleNulls(dumplingRow)

		if err := write(w, fmt.Sprintf("(%s)", strings.Join(row, ", ")), log); err != nil {
			return err
		}

		var splitter string
		if rowIter.HasNext() {
			splitter = ","
		} else {
			splitter = ";"
		}
		if err := write(w, fmt.Sprintf("%s\n", splitter), log); err != nil {
			return err
		}
	}
	log.Debug("finish dumping for table %s", tblIR.TableName())
	return nil
}

func write(writer io.StringWriter, str string, logger Logger) error {
	_, err := writer.WriteString(str)
	if err != nil && logger != nil {
		logger.Error("writing failed, string: `%s`, error: %s", str, err.Error())
	}
	return err
}
