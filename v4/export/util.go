package export

import (
	"fmt"
	"strings"
)

func wrapBackticks(str string) string {
	if strings.HasPrefix(str, "`") && strings.HasSuffix(str, "`") {
		return str
	}
	return fmt.Sprintf("`%s`", str)
}

func handleNulls(origin []string) []string {
	for i, s := range origin {
		if len(s) == 0 {
			origin[i] = "NULL"
		}
	}
	return origin
}
