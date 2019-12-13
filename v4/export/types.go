package export

// Iterator is an iterator tag interface for future extension.
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
	Next(ScanProvider) error
	HasNext() bool
}

// InterRep is an intermediate representation tag interface for future extension.
type InterRep interface {
}

// TableDataIR is table data intermediate representation.
type TableDataIR interface {
	InterRep
	TableName() string
	ColumnNumber() uint

	SpecialComments() StringIter
	Rows() SQLRowIter
}

type ScanProvider interface {
	PrepareScanArgs([]interface{})
}

// Logger used for logging when export.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}

type ErrHandler func(error)

type Context interface {
	GetConfig() *Config
	GetErrorHandler() ErrHandler
}