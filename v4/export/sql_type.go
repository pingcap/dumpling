// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"database/sql"
	"fmt"
)

var colTypeRowReceiverMap = map[string]func() RowReceiverStringer{}

var (
	nullValue           = "NULL"
	quotationMark       = []byte{'\''}
	twoQuotationMarks   = []byte{'\'', '\''}
	doubleQuotationMark = []byte{'"'}
)

func initColTypeRowReceiverMap() {
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

func escapeBackslashFn(s []byte, bf *bytes.Buffer, delimiter []byte, separator []byte) {
	var (
		escape   byte
		last          = 0
		delimit  byte = 0
		separate byte = 0
	)
	if len(delimiter) > 0 {
		delimit = delimiter[0]
	}
	if len(separator) > 0 {
		separate = separator[0]
	}
	// reference: https://gist.github.com/siddontang/8875771
	for i := 0; i < len(s); i++ {
		escape = 0

		switch s[i] {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
		case '\n': /* Must be escaped for logs */
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"': /* Better safe than sorry */
			escape = '"'
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		case delimit:
			escape = delimit
		case separate:
			escape = separate
		}

		if escape != 0 {
			bf.Write(s[last:i])
			bf.WriteByte('\\')
			bf.WriteByte(escape)
			last = i + 1
		}
	}
	if last == 0 {
		bf.Write(s)
	} else if last < len(s) {
		bf.Write(s[last:])
	}
}

func escapeSQL(s []byte, bf *bytes.Buffer, escapeBackslash bool) {
	if escapeBackslash {
		escapeBackslashFn(s, bf, nil, nil)
		return
	}
	bf.Write(bytes.ReplaceAll(s, quotationMark, twoQuotationMarks))
	return
}

func escapeCSV(s []byte, bf *bytes.Buffer, escapeBackslash bool, delimiter []byte, separator []byte) {
	if escapeBackslash {
		escapeBackslashFn(s, bf, delimiter, separator)
		return
	}

	var (
		escape   bool
		last          = 0
		separate byte = 0
	)
	if len(delimiter) > 0 {
		bf.Write(bytes.ReplaceAll(s, delimiter, append(delimiter, delimiter...)))
		return
	}
	if len(separator) > 0 {
		separate = separator[0]
	}

	for i := 0; i < len(s); i++ {
		escape = false
		b := s[i]

		switch b {
		case 0: /* always escape 0 */
			b = '0'
			escape = true
		case '\n': /* Must be escaped for line encloser */
			b = 'n'
			escape = true
		case '\\':
			escape = true
		case separate:
			escape = true
		}

		if escape {
			bf.Write(s[last:i])
			bf.WriteByte('\\')
			bf.WriteByte(b)
			last = i + 1
		}
	}
	if last == 0 {
		bf.Write(s)
	} else if last < len(s) {
		bf.Write(s[last:])
	}
}

// SQLTypeStringMaker returns a SQLTypeString
func SQLTypeStringMaker() RowReceiverStringer {
	return &SQLTypeString{}
}

// SQLTypeBytesMaker returns a SQLTypeBytes
func SQLTypeBytesMaker() RowReceiverStringer {
	return &SQLTypeBytes{}
}

// SQLTypeNumberMaker returns a SQLTypeNumber
func SQLTypeNumberMaker() RowReceiverStringer {
	return &SQLTypeNumber{}
}

// MakeRowReceiver constructs RowReceiverArr from column types
func MakeRowReceiver(colTypes []string) RowReceiverStringer {
	rowReceiverArr := make([]RowReceiverStringer, len(colTypes))
	for i, colTp := range colTypes {
		recMaker, ok := colTypeRowReceiverMap[colTp]
		if !ok {
			recMaker = SQLTypeStringMaker
		}
		rowReceiverArr[i] = recMaker()
	}
	return RowReceiverArr{
		bound:     false,
		receivers: rowReceiverArr,
	}
}

// RowReceiverArr is the combined RowReceiver array
type RowReceiverArr struct {
	bound     bool
	receivers []RowReceiverStringer
}

// BindAddress implements RowReceiver.BindAddress
func (r RowReceiverArr) BindAddress(args []interface{}) {
	if r.bound {
		return
	}
	r.bound = true
	for i := range args {
		r.receivers[i].BindAddress(args[i : i+1])
	}
}

// WriteToBuffer implements Stringer.WriteToBuffer
func (r RowReceiverArr) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	bf.WriteByte('(')
	for i, receiver := range r.receivers {
		receiver.WriteToBuffer(bf, escapeBackslash)
		if i != len(r.receivers)-1 {
			bf.WriteByte(',')
		}
	}
	bf.WriteByte(')')
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (r RowReceiverArr) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	for i, receiver := range r.receivers {
		receiver.WriteToBufferInCsv(bf, escapeBackslash, opt)
		if i != len(r.receivers)-1 {
			bf.Write(opt.separator)
		}
	}
}

// SQLTypeNumber implements RowReceiverStringer which represents numeric type columns in database
type SQLTypeNumber struct {
	SQLTypeString
}

// WriteToBuffer implements Stringer.WriteToBuffer
func (s SQLTypeNumber) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(nullValue)
	}
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (s SQLTypeNumber) WriteToBufferInCsv(bf *bytes.Buffer, _ bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(opt.nullValue)
	}
}

// SQLTypeString implements RowReceiverStringer which represents string type columns in database
type SQLTypeString struct {
	sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress
func (s *SQLTypeString) BindAddress(arg []interface{}) {
	arg[0] = &s.RawBytes
}

// WriteToBuffer implements Stringer.WriteToBuffer
func (s *SQLTypeString) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	if s.RawBytes != nil {
		bf.Write(quotationMark)
		escapeSQL(s.RawBytes, bf, escapeBackslash)
		bf.Write(quotationMark)
	} else {
		bf.WriteString(nullValue)
	}
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (s *SQLTypeString) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(opt.delimiter)
		escapeCSV(s.RawBytes, bf, escapeBackslash, opt.delimiter, opt.separator)
		bf.Write(opt.delimiter)
	} else {
		bf.WriteString(opt.nullValue)
	}
}

// SQLTypeBytes implements RowReceiverStringer which represents bytes type columns in database
type SQLTypeBytes struct {
	sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress
func (s *SQLTypeBytes) BindAddress(arg []interface{}) {
	arg[0] = &s.RawBytes
}

// WriteToBuffer implements Stringer.WriteToBuffer
func (s *SQLTypeBytes) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	if s.RawBytes != nil {
		fmt.Fprintf(bf, "x'%x'", s.RawBytes)
	} else {
		bf.WriteString(nullValue)
	}
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (s *SQLTypeBytes) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(opt.delimiter)
		escapeCSV(s.RawBytes, bf, escapeBackslash, opt.delimiter, opt.separator)
		bf.Write(opt.delimiter)
	} else {
		bf.WriteString(opt.nullValue)
	}
}
