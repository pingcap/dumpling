package export

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
)

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

	tblName := tblIR.TableName()
	if !strings.HasPrefix(tblName, "`") && !strings.HasSuffix(tblName, "`") {
		tblName = wrapStringWith(tblName, "`")
	}
	if err := write(w, fmt.Sprintf("INSERT INTO %s VALUES \n", tblName), log); err != nil {
		return err
	}

	for rowIter.HasNext() {
		row := makeRowReceiver(tblIR.ColumnTypes())
		if err := rowIter.Next(row); err != nil {
			log.Error("scanning from sql.Row failed, error: %s", err.Error())
			return err
		}

		if err := write(w, row.ToString(), log); err != nil {
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

func wrapStringWith(str string, wrapper string) string {
	return fmt.Sprintf("%s%s%s", wrapper, str, wrapper)
}

func SQLTypeStringMaker() RowReceiverStringer {
	return &SQLTypeString{}
}

func SQLTypeBytesMaker() RowReceiverStringer {
	return &SQLTypeBytes{}
}

func SQLTypeNumberMaker() RowReceiverStringer {
	return &SQLTypeInteger{}
}

var colTypeRowReceiverMap = map[string]func() RowReceiverStringer{
	"CHAR":      SQLTypeStringMaker,
	"NCHAR":     SQLTypeStringMaker,
	"VARCHAR":   SQLTypeStringMaker,
	"NVARCHAR":  SQLTypeStringMaker,
	"TEXT":      SQLTypeStringMaker,
	"ENUM":      SQLTypeStringMaker,
	"SET":       SQLTypeStringMaker,
	"JSON":      SQLTypeStringMaker,
	"TIMESTAMP": SQLTypeStringMaker,
	"DATETIME":  SQLTypeStringMaker,
	"DATE":      SQLTypeStringMaker,
	"TIME":      SQLTypeStringMaker,
	"YEAR":      SQLTypeStringMaker,

	"BINARY":    SQLTypeBytesMaker,
	"VARBINARY": SQLTypeBytesMaker,
	"BLOB":      SQLTypeBytesMaker,
	"BIT":       SQLTypeBytesMaker,

	"INT":     SQLTypeNumberMaker,
	"FLOAT":   SQLTypeNumberMaker,
	"DECIMAL": SQLTypeNumberMaker,
}

func makeRowReceiver(colTypes []string) RowReceiverStringer {
	rowReceiverArr := make(RowReceiverArr, len(colTypes))
	for i, colTp := range colTypes {
		recMaker, ok := colTypeRowReceiverMap[colTp]
		if !ok {
			recMaker = SQLTypeStringMaker
		}
		rowReceiverArr[i] = recMaker()
	}
	return rowReceiverArr
}

type RowReceiverArr []RowReceiverStringer

func (r RowReceiverArr) BindAddress(args []interface{}) {
	for i := range args {
		var singleAddr [1]interface{}
		r[i].BindAddress(singleAddr[:])
		args[i] = singleAddr[0]
	}
}
func (r RowReceiverArr) ReportSize() uint64 {
	var sum uint64
	for _, receiver := range r {
		sum += receiver.ReportSize()
	}
	return sum
}
func (r RowReceiverArr) ToString() string {
	var sb strings.Builder
	sb.WriteString("(")
	for i, receiver := range r {
		sb.WriteString(receiver.ToString())
		if i != len(r)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

type SQLTypeInteger struct {
	SQLTypeString
}

func (s SQLTypeInteger) ToString() string {
	if s.Valid {
		return s.String
	} else {
		return "NULL"
	}
}

type SQLTypeString struct {
	sql.NullString
}

func (s *SQLTypeString) BindAddress(arg []interface{}) {
	arg[0] = s
}
func (s *SQLTypeString) ReportSize() uint64 {
	if s.Valid {
		return uint64(len(s.String))
	}
	return uint64(len("NULL"))
}
func (s *SQLTypeString) ToString() string {
	if s.Valid {
		return fmt.Sprintf(`'%s'`, escapeSingleQuote(s.String))
	} else {
		return "NULL"
	}
}

func escapeSingleQuote(src string) string {
	return strings.ReplaceAll(src, "'", "\\'")
}

type SQLTypeBytes struct {
	bytes []byte
}

func (s *SQLTypeBytes) BindAddress(arg []interface{}) {
	arg[0] = &s.bytes
}
func (s *SQLTypeBytes) ReportSize() uint64 {
	return uint64(len(s.bytes))
}
func (s *SQLTypeBytes) ToString() string {
	return fmt.Sprintf("x'%x'", s.bytes)
}
