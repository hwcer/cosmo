// Package clause 提供了 MongoDB 查询条件构建的功能，支持各种查询操作符和复杂条件组合。
// 该包允许用户通过链式调用构建 MongoDB 查询条件，支持等于、大于、小于、IN、NOT 等操作符，
// 以及 OR、AND、NOT、NOR 等复杂逻辑组合。
package clause

import (
	"encoding/json"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

// 定义MongoDB查询相关的常量
const (
	MongoPrimaryName     = "_id" // MongoDB默认主键字段名
	QueryOperationPrefix = "$"   // MongoDB查询操作符前缀
)

// 定义复杂条件操作符常量
const (
	QueryOperationOR  = "or"  // 逻辑或操作符
	QueryOperationAND = "and" // 逻辑与操作符
	QueryOperationNOT = "not" // 逻辑非操作符
	QueryOperationNOR = "nor" // 逻辑或非操作符
)

// complexCondition 是包含所有复杂条件操作符的切片，用于判断条件类型
var complexCondition = []string{QueryOperationOR, QueryOperationAND, QueryOperationNOT, QueryOperationNOR}

// New 创建一个新的查询条件构建器实例。
// 返回值是新的 Query 实例，用于构建 MongoDB 查询条件。
//
// 使用示例：
// query := clause.New()
// query.Eq("name", "John").Gt("age", 18)
func New() *Query {
	q := &Query{}
	q.complex = make(map[string][]*Node)
	return q
}

// Node 表示单个查询条件节点。
// t 是操作符类型（如 $eq, $gt, $lt 等）
// k 是字段名
// v 是比较值
type Node struct {
	t string // 操作符类型
	k string // 字段名
	v any    // 比较值
}

// Query 表示 MongoDB 查询条件构建器，用于构建复杂的查询条件。
// filter 是查询过滤器
// where 是简单条件节点列表
// complex 是复杂条件（or, and, not, nor）的节点映射
type Query struct {
	filter  Filter             // 查询过滤器
	where   []*Node            // 简单条件节点列表
	complex map[string][]*Node // 复杂条件节点映射
}

// Len 返回查询条件中节点的总数，包括简单条件和复杂条件。
// 返回值是查询条件中所有节点的数量。
//
// 使用示例：
// query := clause.New()
// query.Eq("name", "John").Gt("age", 18)
// fmt.Println(query.Len()) // 输出: 2
func (q *Query) Len() (r int) {
	r += len(q.where)
	for _, n := range q.complex {
		r += len(n)
	}
	return
}

// Primary 使用主键字段（_id）进行匹配，可以匹配单个值或值数组。
// 参数 v 是要匹配的主键值或值数组。
//
// 使用示例：
// query := clause.New()
// query.Primary("5f7d8e9a0b1c2d3e4f5a6b7c") // 单个主键值
// 或
// query.Primary([]string{"id1", "id2", "id3"}) // 多个主键值
func (q *Query) Primary(v interface{}) {
	q.Eq(MongoPrimaryName, v)
}

// any 是内部方法，用于向查询中添加任意类型的条件节点。
// 参数 t 是操作符类型（如 eq, gt, lt 等）
// 参数 k 是字段名
// 参数 v 是比较值
// 如果操作符类型不是以 $ 开头，会自动添加 $ 前缀。
func (q *Query) any(t, k string, v interface{}) {
	if !strings.HasPrefix(t, QueryOperationPrefix) {
		t = QueryOperationPrefix + t
	}
	q.where = append(q.where, &Node{t: t, k: k, v: v})
}

// match 是内部方法，用于添加复杂条件匹配（or, not, and, nor）。
// 参数 t 是复杂条件类型
// 参数 v 是条件节点列表
// 如果条件类型以 $ 开头，会自动移除 $ 前缀。
func (q *Query) match(t string, v ...*Node) {
	t = strings.TrimPrefix(t, QueryOperationPrefix)
	q.complex[t] = append(q.complex[t], v...)
}

// Eq 添加等于（$eq）条件匹配。
// 参数 k 是字段名
// 参数 v 是要匹配的值
//
// 使用示例：
// query := clause.New()
// query.Eq("name", "John") // { "name": "John" }
func (q *Query) Eq(k string, v interface{}) {
	q.any("", k, v)
}

// Gt 添加大于（$gt）条件匹配。
// 参数 k 是字段名
// 参数 v 是要比较的值
//
// 使用示例：
// query := clause.New()
// query.Gt("age", 18) // { "age": { "$gt": 18 } }
func (q *Query) Gt(k string, v interface{}) {
	q.any("$gt", k, v)
}

// Gte 添加大于等于（$gte）条件匹配。
// 参数 k 是字段名
// 参数 v 是要比较的值
//
// 使用示例：
// query := clause.New()
// query.Gte("age", 18) // { "age": { "$gte": 18 } }
func (q *Query) Gte(k string, v interface{}) {
	q.any("$gte", k, v)
}

// Lt 添加小于（$lt）条件匹配。
// 参数 k 是字段名
// 参数 v 是要比较的值
//
// 使用示例：
// query := clause.New()
// query.Lt("age", 30) // { "age": { "$lt": 30 } }
func (q *Query) Lt(k string, v interface{}) {
	q.any("$lt", k, v)
}

// Lte 添加小于等于（$lte）条件匹配。
// 参数 k 是字段名
// 参数 v 是要比较的值
//
// 使用示例：
// query := clause.New()
// query.Lte("age", 30) // { "age": { "$lte": 30 } }
func (q *Query) Lte(k string, v interface{}) {
	q.any("$lte", k, v)
}

// Ne 添加不等于（$ne）条件匹配。
// 参数 k 是字段名
// 参数 v 是要比较的值
//
// 使用示例：
// query := clause.New()
// query.Ne("name", "John") // { "name": { "$ne": "John" } }
func (q *Query) Ne(k string, v interface{}) {
	q.any("$ne", k, v)
}

// In 添加 $in 条件匹配，选择字段值等于指定数组中任何值的文档。
// 参数 k 是字段名
// 参数 v 是要匹配的数组
//
// 使用示例：
// query := clause.New()
// query.In("status", []string{"active", "pending"}) // { "status": { "$in": ["active", "pending"] } }
func (q *Query) In(k string, v interface{}) {
	q.any("$in", k, v)
}

// Nin 添加 $nin 条件匹配，选择字段值不在指定数组中或字段不存在的文档。
// 参数 k 是字段名
// 参数 v 是要排除的数组
//
// 使用示例：
// query := clause.New()
// query.Nin("status", []string{"inactive", "deleted"}) // { "status": { "$nin": ["inactive", "deleted"] } }
func (q *Query) Nin(k string, v interface{}) {
	q.any("$nin", k, v)
}

// OR 添加 $or 条件匹配，对两个或多个表达式执行逻辑 OR 操作，选择满足至少一个表达式的文档。
// 参数 v 是要进行 OR 操作的条件节点列表
//
// 使用示例：
// query := clause.New()
// query.OR(
//
//	&clause.Node{t: "$eq", k: "name", v: "John"},
//	&clause.Node{t: "$eq", k: "name", v: "Jane"},
//
// ) // { "$or": [{ "name": "John" }, { "name": "Jane" }] }
func (q *Query) OR(v ...*Node) {
	q.match("or", v...)
}

// NOT 添加 $not 条件匹配，对指定的操作符表达式执行逻辑 NOT 操作，选择不匹配该表达式的文档。
// 这包括不包含该字段的文档。
// 参数 v 是要进行 NOT 操作的条件节点列表
//
// 使用示例：
// query := clause.New()
// query.NOT(
//
//	&clause.Node{t: "$eq", k: "status", v: "inactive"},
//
// ) // { "$not": [{ "status": "inactive" }] }
func (q *Query) NOT(v ...*Node) {
	q.match("not", v...)
}

// AND 添加 $and 条件匹配，对一个或多个表达式执行逻辑 AND 操作，选择满足所有表达式的文档。
// $and 操作符使用短路求值：如果第一个表达式为 false，MongoDB 将不会评估其余表达式。
// 参数 v 是要进行 AND 操作的条件节点列表
//
// 使用示例：
// query := clause.New()
// query.AND(
//
//	&clause.Node{t: "$gt", k: "age", v: 18},
//	&clause.Node{t: "$lt", k: "age", v: 30},
//
// ) // { "$and": [{ "age": { "$gt": 18 } }, { "age": { "$lt": 30 } }] }
func (q *Query) AND(v ...*Node) {
	q.match("and", v...)
}

// NOR 添加 $nor 条件匹配，对一个或多个查询表达式执行逻辑 NOR 操作，选择不满足所有查询表达式的文档。
// 参数 v 是要进行 NOR 操作的条件节点列表
//
// 使用示例：
// query := clause.New()
// query.NOR(
//
//	&clause.Node{t: "$eq", k: "status", v: "active"},
//	&clause.Node{t: "$eq", k: "status", v: "pending"},
//
// ) // { "$nor": [{ "status": "active" }, { "status": "pending" }] }
func (q *Query) NOR(v ...*Node) {
	q.match("nor", v...)
}

// Marshal 将查询条件转换为 BSON 格式的字节数组。
// 返回值是 BSON 格式的字节数组和可能的错误信息。
//
// 使用示例：
// query := clause.New()
// query.Eq("name", "John").Gt("age", 18)
// bsonData, err := query.Marshal()
func (q *Query) Marshal() ([]byte, error) {
	return bson.Marshal(q.Build(nil))
}

// String 将查询条件转换为 JSON 格式的字符串。
// 返回值是 JSON 格式的字符串。
// 如果转换失败，返回空字符串。
//
// 使用示例：
// query := clause.New()
// query.Eq("name", "John").Gt("age", 18)
// fmt.Println(query.String()) // 输出: {"name":"John","age":{"$gt":18}}
func (q *Query) String() string {
	b, _ := json.Marshal(q.Build(nil))
	return string(b)
}
