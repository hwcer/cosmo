package clause

import (
	"encoding/json"
	"strings"

	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/bson"
)

// operationArray 是包含所有需要数组类型参数的操作符的映射
var operationArray = map[string]bool{
	"$in":  true, // $in 操作符需要数组类型参数
	"$nin": true, // $nin 操作符需要数组类型参数
}

// Filter 是 bson.M 的别名，用于表示 MongoDB 查询过滤器。
// 它提供了一系列方法来构建和操作 MongoDB 查询条件。
type Filter bson.M

// Match 添加特殊匹配条件（or, not, and, nor）。
// 参数 t 是匹配类型（如 or, not, and, nor）
// 参数 v 是匹配值
// 如果匹配类型不是以 $ 开头，会自动添加 $ 前缀。
//
// 使用示例：
// filter := clause.Filter{}
// filter.Match("or", bson.M{"name": "John"})
// filter.Match("or", bson.M{"name": "Jane"})
// // 结果: { "$or": [{ "name": "John" }, { "name": "Jane" }] }
func (this Filter) Match(t string, v interface{}) {
	if !strings.HasPrefix(t, "$") {
		t = "$" + t
	}
	arr := utils.ToArray(v)
	if x, ok := this[t]; !ok {
		this[t] = arr
	} else {
		s, _ := x.([]interface{})
		this[t] = append(s, arr...)
	}
}

// Primary 使用主键字段（_id）进行匹配。
// 参数 v 是要匹配的主键值。
//
// 使用示例：
// filter := clause.Filter{}
// filter.Primary("5f7d8e9a0b1c2d3e4f5a6b7c") // 结果: { "_id": "5f7d8e9a0b1c2d3e4f5a6b7c" }
func (this Filter) Primary(v interface{}) {
	this.Eq(MongoPrimaryName, v)
}

// Any 向过滤器添加任意类型的条件。
// 参数 t 是操作符类型（如 eq, gt, lt 等）
// 参数 k 是字段名
// 参数 v 是比较值
// 如果操作符类型不是以 $ 开头，会自动添加 $ 前缀。
// 对于需要数组类型参数的操作符（如 $in, $nin），会自动将值转换为数组。
//
// 使用示例：
// filter := clause.Filter{}
// filter.Any("gt", "age", 18) // 结果: { "age": { "$gt": 18 } }
// filter.Any("in", "status", []string{"active", "pending"}) // 结果: { "status": { "$in": ["active", "pending"] } }
func (this Filter) Any(t, k string, v interface{}) {
	if !strings.HasPrefix(t, "$") {
		t = "$" + t
	}
	var err error
	var data bson.M
	if old, ok := this[k]; !ok {
		data = bson.M{}
		this[k] = data
	} else if data, err = utils.ToBson(old); err != nil {
		data = bson.M{}
		data["$in"] = []interface{}{old}
		this[k] = data
	}
	if operationArray[t] {
		arr := utils.ToArray(v)
		if x, ok := data[t]; !ok {
			data[t] = arr
		} else {
			s, _ := x.([]interface{})
			data[t] = append(s, arr...)
		}
	} else {
		data[t] = v
	}
}

// Eq 添加等于（$eq）条件匹配。
// 参数 k 是字段名
// 参数 v 是要匹配的值
// 如果该字段已经存在条件，则自动转换为 $in 操作符。
//
// 使用示例：
// filter := clause.Filter{}
// filter.Eq("name", "John") // 结果: { "name": "John" }
// filter.Eq("name", "Jane") // 结果: { "name": { "$in": ["John", "Jane"] } }
func (this Filter) Eq(k string, v interface{}) {
	if _, ok := this[k]; !ok {
		this[k] = v
	} else {
		this.In(k, v)
	}
}

// Gt 添加大于（$gt）条件匹配。
// 参数 k 是字段名
// 参数 v 是要比较的值
//
// 使用示例：
// filter := clause.Filter{}
// filter.Gt("age", 18) // 结果: { "age": { "$gt": 18 } }
func (this Filter) Gt(k string, v interface{}) {
	this.Any("$gt", k, v)
}

// Gte 添加大于等于（$gte）条件匹配。
// 参数 k 是字段名
// 参数 v 是要比较的值
//
// 使用示例：
// filter := clause.Filter{}
// filter.Gte("age", 18) // 结果: { "age": { "$gte": 18 } }
func (this Filter) Gte(k string, v interface{}) {
	this.Any("$gte", k, v)
}

// Lt 小于（<）
func (this Filter) Lt(k string, v interface{}) {
	this.Any("$lt", k, v)
}

// Lte 小于等于（<=）
func (this Filter) Lte(k string, v interface{}) {
	this.Any("$lte", k, v)
}

// Ne 不等于（!=）
func (this Filter) Ne(k string, v interface{}) {
	this.Any("$ne", k, v)
}

// In The $in operator selects the documents where the value of a field equals any value in the specified array
func (this Filter) In(k string, v interface{}) {
	this.Any("$in", k, v)
}

// Nin selects the documents where: the field value is not in the specified array or the field does not exist.
func (this Filter) Nin(k string, v ...interface{}) {
	this.Any("$nin", k, v)
}

// OR The $or operator performs a logical OR operation on an array of two or more <expressions> and selects the documents that satisfy at least one of the <expressions>.
func (this Filter) OR(v interface{}) {
	this.Match("$or", v)
}

// NOT $not performs a logical NOT operation on the specified <operator-expression> and selects the documents that do not match the <operator-expression>.
// This includes documents that do not contain the field.
func (this Filter) NOT(v interface{}) {
	this.Match("$not", v)
}

// AND $and performs a logical AND operation on an array of one or more expressions (e.g. <expression1>, <expression2>, etc.) and selects the documents that satisfy all the expressions in the array.
// The $and operator uses short-circuit evaluation. If the first expression (e.g. <expression1>) evaluates to false, MongoDB will not evaluate the remaining expressions.
func (this Filter) AND(v interface{}) {
	this.Match("$and", v)
}

// NOR $nor performs a logical NOR operation on an array of one or more query expression and selects the documents that fail all the query expressions in the array.
func (this Filter) NOR(v interface{}) {
	this.Match("$nor", v)
}

func (this Filter) Merge(src Filter) {
	for k, v := range src {
		this[k] = v
	}
}

func (this Filter) Marshal() ([]byte, error) {
	return bson.Marshal(this)
}

func (this Filter) String() string {
	b, _ := json.Marshal(this)
	return string(b)
}
