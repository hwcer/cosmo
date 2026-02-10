// Package clause 提供查询条件构建功能，支持SQL风格的查询条件解析
// 用于构建MongoDB的查询条件，支持多种查询格式和复杂的条件组合
package clause

import (
	"strings"

	"github.com/hwcer/logger"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// sqlConditionSplit SQL语法分隔符，用于解析SQL风格的查询条件
const sqlConditionSplit = " "

// Where 相关变量定义
var (
	// whereComplexMap SQL风格复杂条件映射（如" OR " => "or"）
	whereComplexMap = make(map[string]string)

	// whereConditionArr 支持的条件操作符列表（按优先级排序，长操作符优先匹配）
	whereConditionArr = []string{"NIN", "IN", "!=", "<>", ">=", "<=", ">", "<", "="}

	// whereConditionSql SQL风格条件操作符映射，用于在查询字符串中识别操作符
	whereConditionSql = make(map[string]string)

	// whereConditionMongo SQL条件到MongoDB操作符的映射
	// 例如："=" -> "", "!=" -> "nin", ">=" -> "gte"
	whereConditionMongo = map[string]string{
		"=":   "",    // 等于条件在MongoDB中不需要特殊操作符
		"!=":  "nin", // 不等于等价于不在数组中
		"<>":  "nin", // 不等于（另一种表示）
		">=":  "gte", // 大于等于
		"<=":  "lte", // 小于等于
		">":   "gt",  // 大于
		"<":   "lt",  // 小于
		"IN":  "in",  // 在数组中
		"NIN": "nin", // 不在数组中
	}
)

// isArrCondition 判断条件是否为数组类型条件（IN/NIN）
// 参数 k: 条件操作符
// 返回值: true表示是数组类型条件（IN或NIN）
func isArrCondition(k string) bool {
	return k == "IN" || k == "NIN"
}

// init 初始化Where相关的映射表
// 自动在包加载时执行，初始化复杂条件映射和SQL条件操作符映射
func init() {
	// 初始化复杂条件映射（AND/OR/NOT/NOR）
	// 将操作符转换为大写并添加空格分隔符，便于在查询字符串中识别
	for _, k := range complexCondition {
		pair := []string{"", strings.ToUpper(k), ""}
		whereComplexMap[k] = strings.Join(pair, sqlConditionSplit)
	}

	// 初始化SQL条件操作符映射
	// 为数组类型条件（IN/NIN）添加空格分隔符，普通条件直接使用操作符
	for _, k := range whereConditionArr {
		if isArrCondition(k) {
			pair := []string{"", strings.ToUpper(k), ""}
			whereConditionSql[k] = strings.Join(pair, sqlConditionSplit)
		} else {
			whereConditionSql[k] = k
		}
	}
}

// IsQueryFormat 判断字符串是否为SQL风格的查询条件格式
// 检查字符串是否包含任何支持的条件操作符
// 参数 s: 要检查的字符串
// 返回值: true表示是SQL风格的查询条件格式
func IsQueryFormat(s string) bool {
	for _, k := range whereConditionArr {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

// fromMap 从map[string]any构建查询条件
// 将map中的每个键值对转换为等于条件（$eq）
// 参数 data: 包含字段名和值的map
func (q *Query) fromMap(data map[string]any) {
	for k, v := range data {
		q.Eq(k, v)
	}
}

// fromFilter 从bson.M构建查询条件
// 直接将bson.M中的键值对添加到查询过滤器中
// 参数 data: bson.M格式的查询条件
func (q *Query) fromFilter(data Filter) {
	if q.filter == nil {
		q.filter = data
	} else {
		q.filter.Merge(data)
	}
}

// formPrimary 根据主键值构建查询条件
// 根据主键值的类型选择合适的查询方式
// 参数 i: 主键值，可以是单个值或数组
func (q *Query) formPrimary(i any) {
	switch i.(type) {
	// 单个主键值（基本数据类型），使用等于条件（$eq）
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		q.Eq(MongoPrimaryName, i)
	default:
		// 数组类型主键值，使用IN条件（$in）
		q.In(MongoPrimaryName, i)
	}
}

// formString 根据字段名和值构建查询条件
// 根据值的类型选择合适的查询方式
// 参数 k: 字段名
// 参数 v: 字段值，可以是单个值或数组
func (q *Query) formString(k string, v any) {
	switch v.(type) {
	// 单个值（基本数据类型），使用等于条件（$eq）
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		q.Eq(k, v)
	default:
		// 数组类型值，使用IN条件（$in）
		q.In(k, v)
	}
}

// formClause 从SQL风格的查询字符串构建查询条件
// 解析SQL风格的查询字符串，支持复杂条件和参数替换
// 参数 query: SQL风格的查询字符串（如"name = ? AND age > ?"）
// 参数 args: 查询参数，用于替换query中的"?"占位符
func (q *Query) formClause(query string, args []any) {
	var arr []string
	var whereType string

	// 检测查询字符串中是否包含复杂条件（AND/OR/NOT/NOR）
	// 根据复杂条件将查询字符串拆分为多个条件片段
	for _, k := range complexCondition {
		if strings.Contains(query, whereComplexMap[k]) {
			whereType = k
			arr = strings.Split(query, whereComplexMap[k])
			break
		}
	}

	// 如果没有复杂条件，默认使用AND连接
	// 将整个查询字符串作为一个条件片段
	if len(arr) == 0 {
		arr = append(arr, query)
		whereType = QueryOperationAND
	}

	var nodes []*Node
	var argIndex int = 0

	// 解析每个条件片段
	for _, pair := range arr {
		var v interface{}
		// 如果条件片段包含"?"占位符，使用args中的参数替换
		if strings.Contains(pair, "?") && argIndex < len(args) {
			v = args[argIndex]
			argIndex += 1
		}

		// 检测并解析条件操作符（如=, !=, >, <等）
		// 按优先级顺序检查，确保长操作符（如IN, >=）先被匹配
		for _, w := range whereConditionArr {
			if strings.Contains(pair, whereConditionSql[w]) {
				// 解析条件对并创建条件节点
				if node := parseWherePair(pair, w, v); node != nil {
					nodes = append(nodes, node)
				}
				break
			}
		}
	}

	// 将解析后的条件节点添加到查询中
	if whereType == QueryOperationAND {
		// AND条件直接添加到where列表中
		q.where = append(q.where, nodes...)
	} else {
		// 其他条件类型（OR/NOT/NOR）使用match方法处理
		q.match(whereType, nodes...)
	}
}

// Where 构造查询条件，支持多种格式
// 支持的格式：
// 1. SQL风格字符串: Where("name = ? AND age > ?", "John", 18)
// 2. 主键值: Where("5f8d0d55b54764421b715620")
// 3. 字段名和值: Where("name", "John") 或 Where("age", []int{18, 20, 22})
// 4. Filter类型: Where(Filter{"name": "John"})
// 5. map[string]any类型: Where(map[string]any{"name": "John"})
// 6. bson.M类型: Where(bson.M{"name": "John"})
//
// 参数 format: 查询条件格式
// 参数 cons: 查询参数
func (q *Query) Where(format any, cons ...any) {
	switch k := format.(type) {
	case string:
		if IsQueryFormat(k) {
			// SQL风格查询条件
			q.formClause(k, cons)
		} else if l := len(cons); l == 0 {
			// 主键值
			q.formPrimary(k)
		} else if l == 1 {
			// 单个字段和值
			q.formString(k, cons[0])
		} else {
			// 字段和值数组
			q.formString(k, cons)
		}
	case map[string]any:
		// map[string]any类型查询条件
		q.fromMap(k)
	case bson.M:
		// bson.M类型查询条件
		q.fromFilter(Filter(k))
	case Filter:
		// Filter类型查询条件
		q.fromFilter(Filter(k))
	default:
		if len(cons) == 0 {
			// 默认作为主键值处理
			q.formPrimary(k)
		} else {
			logger.Alert("Query where format type unknown:%v", format)
		}
	}
}

// parseWherePair 解析SQL风格的条件对（如"name = ?"）
// 将条件对字符串拆分为字段名、操作符和值，并转换为MongoDB条件节点
// 参数 pair: 条件对字符串（如"name = ?"或"age > 18"）
// 参数 w: 条件操作符（如=, !=, >等）
// 参数 v: 条件值（用于替换"?"占位符）
// 返回值: 解析后的条件节点，包含字段名、操作符类型和值
func parseWherePair(pair string, w string, v interface{}) *Node {
	// 将条件对字符串按操作符拆分为字段名和值两部分
	arr := strings.Split(pair, w)
	if len(arr) != 2 {
		return nil // 格式错误，无法解析
	}

	// 创建条件节点
	node := &Node{}
	// 设置节点类型（操作符），如$eq, $gt, $gte等
	node.t = QueryOperationPrefix + whereConditionMongo[w]
	// 提取并清理字段名
	node.k = strings.Trim(arr[0], sqlConditionSplit)

	var r interface{}
	// 提取并清理值部分
	r = strings.Trim(arr[1], sqlConditionSplit)
	// 如果值是"?"占位符，则使用传入的参数值替换
	if r == "?" {
		r = v
	} else {
		// 否则格式化值（如将字符串"int(123)"转换为整数123）
		r = formatWhereValue(r)
	}

	node.v = r
	return node
}

// formatWhereValue 格式化查询条件值
// 支持将字符串值转换为相应的基本数据类型（如"int(123)" => 123）
// 参数 v: 原始值（通常是字符串类型）
// 返回值: 格式化后的值（可能转换为其他数据类型）
func formatWhereValue(v any) any {
	// 只有字符串类型的值需要格式化
	s, ok := v.(string)
	if !ok {
		return v
	}

	// 检查是否需要类型转换（如"int(123)" => 123）
	// 遍历支持的类型转换函数映射
	for t, f := range formatWhereTypes {
		if strings.HasPrefix(s, t) {
			// 调用对应的类型转换函数
			return f(t, s)
		}
	}
	// 如果不需要类型转换，返回原始字符串值
	return v
}
