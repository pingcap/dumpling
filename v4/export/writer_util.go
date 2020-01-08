package export

import (
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/dumpling/v4/log"
)

func WriteMeta(meta MetaIR, w io.StringWriter) error {
	logger := log.Sugar()
	logger.Debugf("start dumping meta data for target %s", meta.TargetName())

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(w, fmt.Sprintf("%s;\n", meta.MetaSQL())); err != nil {
		return err
	}

	logger.Debugf("finish dumping meta data for target %s", meta.TargetName())
	return nil
}

func WriteInsert(tblIR TableDataIR, w io.StringWriter) error {
	logger := log.Sugar()
	rowIter := tblIR.Rows()
	if !rowIter.HasNext() {
		return nil
	}

	logger.Debugf("start dumping for table %s", tblIR.TableName())
	specCmtIter := tblIR.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	tblName := wrapBackTicks(tblIR.TableName())
	if err := write(w, fmt.Sprintf("INSERT INTO %s VALUES\n", tblName)); err != nil {
		return err
	}

	for rowIter.HasNext() {
		row := MakeRowReceiver(tblIR.ColumnTypes())
		if err := rowIter.Next(row); err != nil {
			logger.Errorf("scanning from sql.Row failed, error: %s", err.Error())
			return err
		}

		if err := write(w, row.ToString()); err != nil {
			return err
		}

		var splitter string
		if rowIter.HasNext() {
			splitter = ","
		} else {
			splitter = ";"
		}
		if err := write(w, fmt.Sprintf("%s\n", splitter)); err != nil {
			return err
		}
	}
	logger.Debugf("finish dumping for table %s", tblIR.TableName())
	return nil
}

func write(writer io.StringWriter, str string) error {
	_, err := writer.WriteString(str)
	if err != nil {
		log.Sugar().Errorf("writing string '%s' failed: %s", str, err.Error())
	}
	return err
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
