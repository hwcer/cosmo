package clause

import (
	"fmt"
	"strconv"
	"strings"
)

// formatWhereTypes 存储查询条件值的格式化函数映射表
// key为类型前缀（如"int(", "float("），value为对应的格式化函数
var formatWhereTypes = map[string]formatWhereFunc{}

// formatWhereFunc 定义查询条件值的格式化函数类型
// 参数 t: 类型前缀（如"int(")
// 参数 s: 包含类型前缀的原始字符串值
// 返回值: 格式化后的实际值与解析错误——类型前缀是字面量的唯一合法形态,
// 该形态自身解析失败(如 int(abc))不得静默转 0 产出错误条件

type formatWhereFunc func(t, s string) (any, error)

// init 初始化查询条件值的格式化函数映射
func init() {
	// 注册整数类型的格式化函数
	formatWhereTypes["int("] = formatWhereFuncInt
	formatWhereTypes["int32("] = formatWhereFuncInt
	formatWhereTypes["int64("] = formatWhereFuncInt
	// 注册浮点数类型的格式化函数
	formatWhereTypes["float("] = formatWhereFuncFloat
	formatWhereTypes["float32("] = formatWhereFuncFloat
	formatWhereTypes["float64("] = formatWhereFuncFloat
}

// formatWhereFuncInt 将字符串转换为整数类型
// 参数 t: 类型前缀（如"int(")
// 参数 s: 包含类型前缀的原始字符串值
// 返回值: 转换后的整数
func formatWhereFuncInt(t, s string) (any, error) {
	body := strings.TrimSuffix(strings.TrimPrefix(s, t), ")")
	r, err := strconv.Atoi(body)
	if err != nil {
		return nil, fmt.Errorf("typed literal %q is not a valid int", t+body+")")
	}
	return r, nil
}

// formatWhereFuncFloat 将字符串转换为浮点数类型
// 参数 t: 类型前缀（如"float(")
// 参数 s: 包含类型前缀的原始字符串值
// 返回值: 转换后的浮点数
func formatWhereFuncFloat(t, s string) (any, error) {
	body := strings.TrimSuffix(strings.TrimPrefix(s, t), ")")
	r, err := strconv.ParseFloat(body, 64)
	if err != nil {
		return nil, fmt.Errorf("typed literal %q is not a valid float", t+body+")")
	}
	return r, nil
}
