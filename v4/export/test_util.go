package export

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
)

type mockStringWriter struct {
	buf string
}

func (m *mockStringWriter) WriteString(s string) (int, error) {
	if s == "poison" {
		return 0, fmt.Errorf("poison_error")
	}
	m.buf = s
	return len(s), nil
}

type mockStringCollector struct {
	buf string
}

func (m *mockStringCollector) WriteString(s string) (int, error) {
	m.buf += s
	return len(s), nil
}

type mockSQLRowIterator struct {
	idx  int
	data [][]sql.NullString
}

func (m *mockSQLRowIterator) Next(sp RowReceiver) error {
	args := make([]interface{}, len(m.data[m.idx]))
	sp.BindAddress(args)
	for i := range args {
		*(args[i]).(*sql.NullString) = m.data[m.idx][i]
	}
	m.idx += 1
	return nil
}

func (m *mockSQLRowIterator) HasNext() bool {
	return m.idx < len(m.data)
}

type mockMetaIR struct {
	tarName string
	meta    string
	specCmt []string
}

func (m *mockMetaIR) SpecialComments() StringIter {
	return newStringIter(m.specCmt...)
}

func (m *mockMetaIR) TargetName() string {
	return m.tarName
}

func (m *mockMetaIR) MetaSQL() string {
	return m.meta
}

func newMockMetaIR(targetName string, meta string, specialComments []string) MetaIR {
	return &mockMetaIR{
		tarName: targetName,
		meta:    meta,
		specCmt: specialComments,
	}
}

type mockTableIR struct {
	dbName   string
	tblName  string
	data     [][]driver.Value
	specCmt  []string
	colTypes []string
}

func (m *mockTableIR) DatabaseName() string {
	return m.dbName
}

func (m *mockTableIR) TableName() string {
	return m.tblName
}

func (m *mockTableIR) ColumnCount() uint {
	return uint(len(m.colTypes))
}

func (m *mockTableIR) ColumnTypes() []string {
	return m.colTypes
}

func (m *mockTableIR) SpecialComments() StringIter {
	return newStringIter(m.specCmt...)
}

func (m *mockTableIR) Rows() SQLRowIter {
	mockRows := sqlmock.NewRows(m.colTypes)
	for _, datum := range m.data {
		mockRows.AddRow(datum...)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(fmt.Sprintf("sqlmock.New return error: %v", err))
	}
	defer db.Close()
	mock.ExpectQuery("select 1").WillReturnRows(mockRows)
	rows, err := db.Query("select 1")
	if err != nil {
		panic(fmt.Sprintf("sqlmock.New return error: %v", err))
	}

	return newRowIter(rows, len(m.colTypes))
}

func newMockTableIR(databaseName, tableName string, data [][]driver.Value, specialComments, colTypes []string) TableDataIR {
	return &mockTableIR{
		dbName:   databaseName,
		tblName:  tableName,
		data:     data,
		specCmt:  specialComments,
		colTypes: colTypes,
	}
}

type DummyLogger struct{}

func (d *DummyLogger) Debug(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Info(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Warn(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}

func (d *DummyLogger) Error(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
}
