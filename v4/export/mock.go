//+build !codes

package export

import (
	"fmt"
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

type mockStringIter struct {
	idx int
	ss []string
}

func newMockStringIter(ss ...string) StringIter {
	return &mockStringIter{
		idx: 0,
		ss:  ss,
	}
}

func (m *mockStringIter) Next() string {
	if m.idx >= len(m.ss) {
		return ""
	}
	ret := m.ss[m.idx]
	m.idx += 1
	return ret
}

func (m *mockStringIter) HasNext() bool {
	return m.idx < len(m.ss)
}

type mockSQLRowIterator struct {
	idx int
	data [][]string
}

func (m *mockSQLRowIterator) Next(sp ScanProvider) error {
	args := make([]interface{}, len(m.data[m.idx]))
	sp.PrepareScanArgs(args)
	for i := range args {
		*(args[i]).(*string) = m.data[m.idx][i]
	}
	m.idx += 1
	return nil
}

func (m *mockSQLRowIterator) HasNext() bool {
	return m.idx < len(m.data)
}

type mockTableDataIR struct {
	tblName string
	data [][]string
	specCmt []string
}

func newMockTableDataIR(tableName string, data [][]string, specialComments []string) TableDataIR {
	return &mockTableDataIR{
		tblName: tableName,
		data:    data,
		specCmt: specialComments,
	}
}

func (m *mockTableDataIR) TableName() string {
	return "employee"
}

func (m *mockTableDataIR) ColumnNumber() uint {
	return 5
}

func (m *mockTableDataIR) SpecialComments() StringIter {
	return newMockStringIter("/*!40101 SET NAMES binary*/;", "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;")
}

func (m *mockTableDataIR) Rows() SQLRowIter {
	return &mockSQLRowIterator{
		idx:  0,
		data: m.data,
	}
}

type mockContext struct {
	config *Config
	errHandler ErrHandler
}

func (m *mockContext) GetConfig() *Config {
	return m.config
}

func (m *mockContext) GetErrorHandler() ErrHandler {
	return m.errHandler
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
