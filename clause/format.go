package clause

import (
	"strconv"
	"strings"
)

var formatWhereTypes = map[string]formatWhereFunc{}

type formatWhereFunc func(t, s string) any

func init() {
	formatWhereTypes["int("] = formatWhereFuncInt
	formatWhereTypes["int32("] = formatWhereFuncInt
	formatWhereTypes["int64("] = formatWhereFuncInt
	formatWhereTypes["float("] = formatWhereFuncFloat
	formatWhereTypes["float32("] = formatWhereFuncFloat
	formatWhereTypes["float64("] = formatWhereFuncFloat
}

func formatWhereFuncInt(t, s string) any {
	s = strings.TrimPrefix(s, t)
	s = strings.TrimSuffix(s, ")")
	r, _ := strconv.Atoi(s)
	return r
}
func formatWhereFuncFloat(t, s string) any {
	s = strings.TrimPrefix(s, t)
	s = strings.TrimSuffix(s, ")")
	r, _ := strconv.ParseFloat(s, 64)
	return r
}
