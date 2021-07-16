// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	tcontext "github.com/pingcap/dumpling/v4/context"
	"github.com/pingcap/dumpling/v4/log"

	"github.com/pingcap/br/pkg/storage"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var appLogger log.Logger

func TestT(t *testing.T) {
	initColTypeRowReceiverMap()
	logger, _, err := log.InitAppLogger(&log.Config{
		Level:  "debug",
		File:   "",
		Format: "text",
	})
	if err != nil {
		t.Log("fail to init logger, err: " + err.Error())
		t.Fail()
	}
	appLogger = logger
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())
	RegisterMetrics(registry)
	TestingT(t)
}

var _ = SerialSuites(&testWriteSuite{})

type testWriteSuite struct {
	mockCfg *Config
}

func (s *testWriteSuite) SetUpSuite(_ *C) {
	s.mockCfg = &Config{
		FileSize: UnspecifiedSize,
	}
	InitMetricsVector(s.mockCfg.Labels)
}

func (s *testWriteSuite) TearDownTest(c *C) {
	RemoveLabelValuesWithTaskInMetrics(s.mockCfg.Labels)

	c.Assert(ReadGauge(finishedRowsGauge, s.mockCfg.Labels), Equals, float64(0))
	c.Assert(ReadGauge(finishedSizeGauge, s.mockCfg.Labels), Equals, float64(0))
}

func (s *testWriteSuite) TestWriteMeta(c *C) {
	createTableStmt := "CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	specCmts := []string{"/*!40103 SET TIME_ZONE='+00:00' */;"}
	meta := newMockMetaIR("t1", createTableStmt, specCmts)
	writer := storage.NewBufferWriter()

	err := WriteMeta(tcontext.Background(), meta, writer)
	c.Assert(err, IsNil)
	expected := "/*!40103 SET TIME_ZONE='+00:00' */;\n" +
		"CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	c.Assert(writer.String(), Equals, expected)
}

func (s *testWriteSuite) TestWriteInsert(c *C) {
	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	bf := storage.NewBufferWriter()
	writeSpeedLimiter := NewWriteSpeedLimiter(UnspecifiedSize)

	conf := configForWriteSQL(s.mockCfg, UnspecifiedSize, UnspecifiedSize)
	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	c.Assert(n, Equals, uint64(4))
	c.Assert(err, IsNil)
	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy'),\n" +
		"(4,'female','sarah@mail.com','020-1235','healthy');\n"
	c.Assert(bf.String(), Equals, expected)
	c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(len(data)))
	c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(len(expected)))
}

func (s *testWriteSuite) TestWriteInsertReturnsError(c *C) {
	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	// row errors at last line
	rowErr := errors.New("mock row error")
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	tableIR.rowErr = rowErr
	bf := storage.NewBufferWriter()
	writeSpeedLimiter := NewWriteSpeedLimiter(UnspecifiedSize)

	conf := configForWriteSQL(s.mockCfg, UnspecifiedSize, UnspecifiedSize)
	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	c.Assert(n, Equals, uint64(3))
	c.Assert(err, Equals, rowErr)
	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy');\n"
	c.Assert(bf.String(), Equals, expected)
	// error occurred, should revert pointer to zero
	c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(0))
	c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(0))
}

func (s *testWriteSuite) TestWriteInsertInCsv(c *C) {
	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
	bf := storage.NewBufferWriter()
	writeSpeedLimiter := NewWriteSpeedLimiter(UnspecifiedSize)

	// test nullValue
	opt := &csvOption{separator: []byte(","), delimiter: doubleQuotationMark, nullValue: "\\N"}
	conf := configForWriteCSV(s.mockCfg, true, opt)
	n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	c.Assert(n, Equals, uint64(4))
	c.Assert(err, IsNil)
	expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\\N\n" +
		"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"healthy\"\n" +
		"3,\"male\",\"john@mail.com\",\"020-1256\",\"healthy\"\n" +
		"4,\"female\",\"sarah@mail.com\",\"020-1235\",\"healthy\"\n"
	c.Assert(bf.String(), Equals, expected)
	c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(len(data)))
	c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(len(expected)))
	RemoveLabelValuesWithTaskInMetrics(conf.Labels)

	// test delimiter
	bf.Reset()
	opt.delimiter = quotationMark
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	conf = configForWriteCSV(s.mockCfg, true, opt)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	c.Assert(n, Equals, uint64(4))
	c.Assert(err, IsNil)
	expected = "1,'male','bob@mail.com','020-1234',\\N\n" +
		"2,'female','sarah@mail.com','020-1253','healthy'\n" +
		"3,'male','john@mail.com','020-1256','healthy'\n" +
		"4,'female','sarah@mail.com','020-1235','healthy'\n"
	c.Assert(bf.String(), Equals, expected)
	c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(len(data)))
	c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(len(expected)))
	RemoveLabelValuesWithTaskInMetrics(conf.Labels)

	// test separator
	bf.Reset()
	opt.separator = []byte(";")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	conf = configForWriteCSV(s.mockCfg, true, opt)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	c.Assert(n, Equals, uint64(4))
	c.Assert(err, IsNil)
	expected = "1;'male';'bob@mail.com';'020-1234';\\N\n" +
		"2;'female';'sarah@mail.com';'020-1253';'healthy'\n" +
		"3;'male';'john@mail.com';'020-1256';'healthy'\n" +
		"4;'female';'sarah@mail.com';'020-1235';'healthy'\n"
	c.Assert(bf.String(), Equals, expected)
	c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(len(data)))
	c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(len(expected)))
	RemoveLabelValuesWithTaskInMetrics(conf.Labels)

	// test delimiter that included in values
	bf.Reset()
	opt.separator = []byte("&;,?")
	opt.delimiter = []byte("ma")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	tableIR.colNames = []string{"id", "gender", "email", "phone_number", "status"}
	conf = configForWriteCSV(s.mockCfg, false, opt)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	c.Assert(n, Equals, uint64(4))
	c.Assert(err, IsNil)
	expected = "maidma&;,?magenderma&;,?maemamailma&;,?maphone_numberma&;,?mastatusma\n" +
		"1&;,?mamamalema&;,?mabob@mamail.comma&;,?ma020-1234ma&;,?\\N\n" +
		"2&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1253ma&;,?mahealthyma\n" +
		"3&;,?mamamalema&;,?majohn@mamail.comma&;,?ma020-1256ma&;,?mahealthyma\n" +
		"4&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1235ma&;,?mahealthyma\n"
	c.Assert(bf.String(), Equals, expected)
	c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(len(data)))
	c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(len(expected)))
	RemoveLabelValuesWithTaskInMetrics(conf.Labels)
}

func (s *testWriteSuite) TestWriteInsertInCsvReturnsError(c *C) {
	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}

	// row errors at last line
	rowErr := errors.New("mock row error")
	tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
	tableIR.rowErr = rowErr
	bf := storage.NewBufferWriter()
	writeSpeedLimiter := NewWriteSpeedLimiter(UnspecifiedSize)

	// test nullValue
	opt := &csvOption{separator: []byte(","), delimiter: doubleQuotationMark, nullValue: "\\N"}
	conf := configForWriteCSV(s.mockCfg, true, opt)
	n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	c.Assert(n, Equals, uint64(3))
	c.Assert(err, Equals, rowErr)
	expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\\N\n" +
		"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"healthy\"\n" +
		"3,\"male\",\"john@mail.com\",\"020-1256\",\"healthy\"\n"
	c.Assert(bf.String(), Equals, expected)
	c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(0))
	c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(0))
	RemoveLabelValuesWithTaskInMetrics(conf.Labels)
}

func (s *testWriteSuite) TestWriteInsertSpeedLimit(c *C) {
	data := [][]driver.Value{
		{"a", "b", "c", "d", "e"},
		{"a", "bb", "c", "d", "e"},
		{"a", "b", "cc", "d", "e"},
		{"a", "b", "c", "ddd", "e"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
	bf := storage.NewBufferWriter()
	writeSpeedLimiter := NewWriteSpeedLimiter(10)
	conf := &Config{FileSize: UnspecifiedSize, StatementSize: UnspecifiedSize}

	start := time.Now()
	_, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
	end := time.Since(start)

	c.Assert(err, IsNil)

	// The resulting file
	//
	// INSERT INTO `employee` VALUES
	// (a,'b','c','d','e'),
	// (a,'bb','c','d','e'),
	// (a,'b','cc','d','e'),
	// (a,'b','c','ddd','e');
	//
	// 5L, 118B
	//
	// Because the speed limit is 10, 118B need 10 to 11 seconds to complete.
	c.Assert(end.Seconds() >= 10 && end.Seconds() <= 11, IsTrue)
}

func (s *testWriteSuite) TestSQLDataTypes(c *C) {
	data := [][]driver.Value{
		{"CHAR", "char1", `'char1'`},
		{"INT", 12345, `12345`},
		{"BINARY", 1234, "x'31323334'"},
	}

	for _, datum := range data {
		sqlType, origin, result := datum[0].(string), datum[1], datum[2].(string)

		tableData := [][]driver.Value{{origin}}
		colType := []string{sqlType}
		tableIR := newMockTableIR("test", "t", tableData, nil, colType)
		bf := storage.NewBufferWriter()
		writeSpeedLimiter := NewWriteSpeedLimiter(0)

		conf := configForWriteSQL(s.mockCfg, UnspecifiedSize, UnspecifiedSize)
		n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, writeSpeedLimiter)
		c.Assert(n, Equals, uint64(1))
		c.Assert(err, IsNil)
		lines := strings.Split(bf.String(), "\n")
		c.Assert(len(lines), Equals, 3)
		c.Assert(lines[1], Equals, fmt.Sprintf("(%s);", result))
		c.Assert(ReadGauge(finishedRowsGauge, conf.Labels), Equals, float64(1))
		c.Assert(ReadGauge(finishedSizeGauge, conf.Labels), Equals, float64(len(bf.String())))
		RemoveLabelValuesWithTaskInMetrics(conf.Labels)
	}
}

func (s *testWriteSuite) TestWrite(c *C) {
	mocksw := &mockPoisonWriter{}
	src := []string{"test", "loooooooooooooooooooong", "poison"}
	exp := []string{"test", "loooooooooooooooooooong", "poison_error"}

	for i, s := range src {
		err := write(tcontext.Background(), mocksw, s)
		if err != nil {
			c.Assert(err.Error(), Equals, exp[i])
		} else {
			c.Assert(s, Equals, mocksw.buf)
			c.Assert(mocksw.buf, Equals, exp[i])
		}
	}
	err := write(tcontext.Background(), mocksw, "test")
	c.Assert(err, IsNil)
}

// cloneConfigForTest clones a dumpling config.
func cloneConfigForTest(conf *Config) *Config {
	clone := &Config{}
	*clone = *conf
	return clone
}

func configForWriteSQL(config *Config, fileSize, statementSize uint64) *Config {
	cfg := cloneConfigForTest(config)
	cfg.FileSize = fileSize
	cfg.StatementSize = statementSize
	return cfg
}

func configForWriteCSV(config *Config, noHeader bool, opt *csvOption) *Config {
	cfg := cloneConfigForTest(config)
	cfg.NoHeader = noHeader
	cfg.CsvNullValue = opt.nullValue
	cfg.CsvDelimiter = string(opt.delimiter)
	cfg.CsvSeparator = string(opt.separator)
	cfg.FileSize = UnspecifiedSize
	return cfg
}
