package out

import (
	"fmt"
	. "github.com/pingcap/check"
	"testing"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&outputSuite{})

type outputSuite struct{}

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

func (s *outputSuite) TestHandleNulls(c *C) {
	src := []string{"255", "", "25535", "computer_science", "male"}
	exp := []string{"255", "NULL", "25535", "computer_science", "male"}
	c.Assert(handleNulls(src), DeepEquals, exp)
}
