package export

import (
	"github.com/pingcap/tidb-tools/pkg/filter"
)

type BWList interface {
	Apply(string, string) bool
}

type BWListMod byte

const (
	NopMode      BWListMod = 0x0
	OldToolsMode BWListMod = 0x1
)

type OldToolsConf struct {
	Rules         *filter.Rules
	CaseSensitive bool
}

type BWListConf struct {
	Mode BWListMod

	OldTools *OldToolsConf
}

type OldToolsBWList struct {
	*filter.Filter
}

func (bw *OldToolsBWList) Apply(schema, table string) bool {
	return bw.Match(&filter.Table{schema, table})
}

type NopeBWList struct{}

func (bw *NopeBWList) Apply(schema, table string) bool {
	return true
}

func NewBWList(conf BWListConf) (BWList, error) {
	switch conf.Mode {
	case OldToolsMode:
		oldToolsConf := conf.OldTools
		oldToolsBWList, err := filter.New(oldToolsConf.CaseSensitive, oldToolsConf.Rules)
		if err != nil {
			return nil, withStack(err)
		}

		return &OldToolsBWList{
			Filter: oldToolsBWList,
		}, nil
	}

	return &NopeBWList{}, nil
}
