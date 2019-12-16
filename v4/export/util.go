package export

import (
	"database/sql"
	"fmt"
	"strings"
)

func wrapBackticks(str string) string {
	if strings.HasPrefix(str, "`") && strings.HasSuffix(str, "`") {
		return str
	}
	return fmt.Sprintf("`%s`", str)
}

func handleNulls(origin []sql.NullString) []string {
	ret := make([]string, len(origin))
	for i, s := range origin {
		if s.Valid {
			ret[i] = s.String
		} else {
			ret[i] = "NULL"
		}
	}
	return ret
}
