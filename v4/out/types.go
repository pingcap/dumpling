package out

import "database/sql"

// Iterator is a tag interface for future extension.
type Iterator interface{}

// StringIter is the iterator on a collection of strings.
type StringIter interface {
	Iterator
	Next() string
	HasNext() bool
}

// SQLRowIter is the iterator on a collection of sql.Row.
type SQLRowIter interface {
	Iterator
	Next() sql.Row
	HasNext() bool
}

// InterRep is intermediate representation.
type InterRep interface {
}

// TableDataIR is table data intermediate representation.
type TableDataIR interface {
	TableName() string
	ColumnNumber() uint

	SpecialComments() StringIter
	Rows() SQLRowIter
}

// Logger used for logging when output.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}
