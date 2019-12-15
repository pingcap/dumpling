package export

import (
	"io"
	"strings"
)

type dumplingRow []string

func (d dumplingRow) PrepareScanArgs(args []interface{}) {
	for i := range d {
		args[i] = &d[i]
	}
}

func WriteInsert(ctx Context, tblIR TableDataIR, w io.StringWriter) {
	cfg := ctx.GetConfig()
	log := cfg.Logger
	errHandler := ctx.GetErrorHandler()
	rowIter := tblIR.Rows()
	if !rowIter.HasNext() {
		return
	}

	log.Debug("start dumping for table %s", tblIR.TableName())
	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		write(w, specCmtIter.Next(), log, errHandler)
		write(w, cfg.LineSplitter, log, errHandler)
	}

	write(w, "INSERT INTO ", log, errHandler)
	write(w, wrapBackticks(tblIR.TableName()), log, errHandler)
	write(w, " VALUES ", log, errHandler)
	write(w, cfg.LineSplitter, log, errHandler)

	for rowIter.HasNext() {
		var dumplingRow = make(dumplingRow, tblIR.ColumnNumber())
		if err := rowIter.Next(dumplingRow); err != nil {
			log.Error("scanning from sql.Row failed, error: %s", err.Error())
			errHandler(err)
		}

		row := handleNulls(dumplingRow)

		write(w, "(", log, errHandler)
		write(w, strings.Join(row, ", "), log, errHandler)
		write(w, ")", log, errHandler)
		if rowIter.HasNext() {
			write(w, ",", log, errHandler)
		} else {
			write(w, ";", log, errHandler)
		}
		write(w, cfg.LineSplitter, log, errHandler)
	}
	log.Debug("finish dumping for table %s", tblIR.TableName())
}

func write(writer io.StringWriter, str string, logger Logger, errHandler func(error)) {
	_, err := writer.WriteString(str)
	if err == nil {
		return
	}
	if logger != nil {
		logger.Error("writing failed, string: `%s`, error: %s", str, err.Error())
	}
	errHandler(err)
}
