package clause

import (
	"encoding/json"
	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

var operationArray = map[string]bool{
	"$in":  true,
	"$nin": true,
}

type Filter bson.M

// Match 特殊匹配 or,not,and,nor
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

func (this Filter) Primary(v interface{}) {
	this.Eq(MongoPrimaryName, v)
}

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

// Eq 等于（=）
func (this Filter) Eq(k string, v interface{}) {
	if _, ok := this[k]; !ok {
		this[k] = v
	} else {
		this.In(k, v)
	}
}

// Gt 大于（>）
func (this Filter) Gt(k string, v interface{}) {
	this.Any("$gt", k, v)
}

// Gte 大于等于（>=）
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
