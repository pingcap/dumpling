package out

import (
	"io"
	"strings"
)

func WriteInsert(tblIR TableDataIR, writer io.StringWriter, errHandler func(error)) {
	cfg := GetGlobalConfig()
	log := cfg.Logger
	log.Debug("start dumping for table %s", tblIR.TableName())

	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		write(writer, specCmtIter.Next(), log, errHandler)
	}
	write(writer, cfg.LineSplitter, log, errHandler)

	write(writer, "INSERT INTO ", log, errHandler)
	write(writer, WrapBackticks(tblIR.TableName()), log, errHandler)
	write(writer, "VALUES ", log, errHandler)
	write(writer, cfg.LineSplitter, log, errHandler)

	colData := make([]string, tblIR.ColumnNumber())
	colDataPtr := make([]*string, tblIR.ColumnNumber())
	for i := range colDataPtr {
		colDataPtr[i] = &colData[i]
	}
	rowIter := tblIR.Rows()
	for rowIter.HasNext() {
		row := rowIter.Next()
		if err := row.Scan(colDataPtr); err != nil {
			log.Error("scanning from sql.Row failed, error: %s", err.Error())
			errHandler(err)
		}

		colData = handleNulls(colData)

		write(writer, "(", log, errHandler)
		write(writer, strings.Join(colData, ", "), log, errHandler)
		write(writer, "),", log, errHandler)
		write(writer, cfg.LineSplitter, log, errHandler)
	}

	log.Debug("finish dumping for table %s", tblIR.TableName())
}

func handleNulls(origin []string) []string {
	for i, s := range origin {
		if len(s) == 0 {
			origin[i] = "NULL"
		}
	}
	return origin
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
