package out

import (
	"fmt"
	"strings"
)

func WrapBackticks(str string) string {
	if strings.HasPrefix(str, "`") && strings.HasSuffix(str, "`") {
		return str
	}
	return fmt.Sprintf("`%s`", str)
}
