package export

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

var _ = Suite(&testBWListSuite{})

type testBWListSuite struct{}

func (s *testBWListSuite) TestBWList(c *C) {
	nopeBWList, err := NewBWList(BWListConf{})
	c.Assert(err, IsNil)

	c.Assert(nopeBWList.Apply("nope", "nope"), IsTrue)

	oldToolsBWList, err := NewBWList(BWListConf{
		Mode: OldToolsMode,
		OldTools: &OldToolsConf{
			Rules: &filter.Rules{
				DoDBs: []string{"xxx"},
			},
		},
	})
	c.Assert(err, IsNil)

	c.Assert(oldToolsBWList.Apply("xxx", "yyy"), IsTrue)
	c.Assert(oldToolsBWList.Apply("yyy", "xxx"), IsFalse)

	_, err = NewBWList(BWListConf{
		Mode: OldToolsMode,
		OldTools: &OldToolsConf{
			Rules: &filter.Rules{
				DoDBs: []string{""},
			},
		},
	})
	c.Assert(err, NotNil)
}
