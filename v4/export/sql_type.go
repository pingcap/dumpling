package export

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
)

var colTypeRowReceiverMap = map[string]func() RowReceiverStringer{}

func init() {
	for _, s := range dataTypeString {
		colTypeRowReceiverMap[s] = SQLTypeStringMaker
	}
	for _, s := range dataTypeNum {
		colTypeRowReceiverMap[s] = SQLTypeNumberMaker
	}
	for _, s := range dataTypeBin {
		colTypeRowReceiverMap[s] = SQLTypeBytesMaker
	}
}

var dataTypeString = []string{
	"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "CHARACTER", "VARCHARACTER",
	"TIMESTAMP", "DATETIME", "DATE", "TIME", "YEAR", "SQL_TSI_YEAR",
	"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
	"ENUM", "SET", "JSON",
}

var dataTypeNum = []string{
	"INTEGER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT",
	"INT", "INT1", "INT2", "INT3", "INT8",
	"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
	"DECIMAL", "NUMERIC", "FIXED",
	"BOOL", "BOOLEAN",
}

var dataTypeBin = []string{
	"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "LONG",
	"BINARY", "VARBINARY",
	"BIT",
}

func SQLTypeStringMaker() RowReceiverStringer {
	return &SQLTypeString{}
}

func SQLTypeBytesMaker() RowReceiverStringer {
	return &SQLTypeBytes{}
}

func SQLTypeNumberMaker() RowReceiverStringer {
	return &SQLTypeNumber{}
}

func MakeRowReceiver(colTypes []string) RowReceiverStringer {
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
		r[i].BindAddress(args[i:i+1])
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

func (r RowReceiverArr) WriteToStringBuilder (bf *bytes.Buffer) {
	bf.WriteString("(")
	for i, receiver := range r {
		receiver.WriteToStringBuilder(bf)
		if i != len(r)-1 {
			bf.WriteString(", ")
		}
	}
	bf.WriteString(")")
}


type SQLTypeNumber struct {
	SQLTypeString
}

func (s SQLTypeNumber) ToString() string {
	if s.Valid {
		return s.String
	} else {
		return "NULL"
	}
}

func (s SQLTypeNumber) WriteToStringBuilder (bf *bytes.Buffer) {
	if s.Valid {
		bf.WriteString(s.String)
	} else {
		bf.WriteString("NULL")
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
		return fmt.Sprintf(`'%s'`, escape(s.String))
	} else {
		return "NULL"
	}
}

func (s *SQLTypeString) WriteToStringBuilder (bf *bytes.Buffer) {
	if s.Valid {
		l := len(s.String)
		var escape byte
		for i := 0; i < l; i++ {
			c := s.String[i]

			escape = 0
			switch c {
			case '\\':
				escape = '\\'
				break
			case '\'':
				escape = '\''
				break
			}

			if escape != 0 {
				bf.WriteByte(escape)
			}
			bf.WriteByte(c)
		}
	} else {
		bf.WriteString("NULL")
	}
}

func escape(src string) string {
	src = strings.ReplaceAll(src, "'", "''")
	return strings.ReplaceAll(src, `\`, `\\`)
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

func (s *SQLTypeBytes) WriteToStringBuilder (bf *bytes.Buffer) {
	bf.WriteString(fmt.Sprintf("x'%x'", s.bytes))
}