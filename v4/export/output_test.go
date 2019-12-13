package export

import (
	. "github.com/pingcap/check"
	"testing"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&outputSuite{})

type outputSuite struct{}

func (s *outputSuite) TestWriteInsert(c *C) {
	data := [][]string{
		{"1", "male", "bob@mail.com", "020-1234", ""},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	specCmts := []string {
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	tableIR := newMockTableDataIR("employee", data, specCmts)
	ctx := &mockContext{config: &Config{
		LineSplitter: "\n",
		Logger:       &DummyLogger{},
	}, errHandler: func(error){}}
	strCollector := &mockStringCollector{}

	WriteInsert(ctx, tableIR, strCollector)
	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES \n" +
		"(1, male, bob@mail.com, 020-1234, NULL),\n" +
		"(2, female, sarah@mail.com, 020-1253, healthy),\n" +
		"(3, male, john@mail.com, 020-1256, healthy),\n" +
		"(4, female, sarah@mail.com, 020-1235, healthy);\n"
	c.Assert(strCollector.buf, Equals, expected)
}

func (s *outputSuite) TestWrite(c *C) {
	mocksw := &mockStringWriter{}
	src := []string{"test", "loooooooooooooooooooong", "poison"}
	exp := []string{"test", "loooooooooooooooooooong", "poison_error"}

	for i, s := range src {
		containsErr := false
		write(mocksw, s, nil, func(err error) {
			containsErr = true
			c.Assert(exp[i], Equals, err.Error())
		})
		if !containsErr {
			c.Assert(s, Equals, mocksw.buf)
			c.Assert(exp[i], Equals, mocksw.buf)
		}
	}
	write(mocksw, "test", nil, func(error) {})

}
