package clause

import (
	"github.com/hwcer/logger"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

const sqlConditionSplit = " " //SQL语法分隔符

// Where 构造查询条件
// 支持 =,>,<,>=,<=,<>,!=
// 支持使用OR,AND,NOT,NOR连接多个条件，OR,AND,NOT,NOR一次只能拼接一种
var whereComplexMap = make(map[string]string)
var whereConditionArr = []string{"NIN", "IN", "!=", "<>", ">=", "<=", ">", "<", "="}
var whereConditionSql = make(map[string]string)
var whereConditionMongo = map[string]string{
	"=":   "",
	"!=":  "nin",
	"<>":  "nin",
	">=":  "gte",
	"<=":  "lte",
	">":   "gt",
	"<":   "lt",
	"IN":  "in",
	"NIN": "nin",
}

func isArrCondition(k string) bool {
	return k == "IN" || k == "NIN"
}

func init() {
	for _, k := range complexCondition {
		pair := []string{"", strings.ToUpper(k), ""}
		whereComplexMap[k] = strings.Join(pair, sqlConditionSplit)
	}

	for _, k := range whereConditionArr {
		if isArrCondition(k) {
			pair := []string{"", strings.ToUpper(k), ""}
			whereConditionSql[k] = strings.Join(pair, sqlConditionSplit)
		} else {
			whereConditionSql[k] = k
		}
	}

}

func IsQueryFormat(s string) bool {
	for _, k := range whereConditionArr {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

func (q *Query) fromMap(data map[string]any) {
	for k, v := range data {
		q.Eq(k, v)
	}
}

func (q *Query) formPrimary(i any) {
	switch i.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		q.Eq(MongoPrimaryName, i)
	default:
		q.In(MongoPrimaryName, i)
	}
}
func (q *Query) formString(k string, v any) {
	switch v.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		q.Eq(k, v)
	default:
		q.In(k, v)
	}
}
func (q *Query) formClause(query string, args []any) {
	var arr []string
	var whereType string
	for _, k := range complexCondition {
		if strings.Contains(query, whereComplexMap[k]) {
			whereType = k
			arr = strings.Split(query, whereComplexMap[k])
			break
		}
	}
	if len(arr) == 0 {
		arr = append(arr, query)
		whereType = QueryOperationAND
	}

	var nodes []*Node
	var argIndex int = 0

	for _, pair := range arr {
		var v interface{}
		if strings.Contains(pair, "?") && argIndex < len(args) {
			v = args[argIndex]
			argIndex += 1
		}
		for _, w := range whereConditionArr {
			if strings.Contains(pair, whereConditionSql[w]) {
				if node := parseWherePair(pair, w, v); node != nil {
					nodes = append(nodes, node)
				}
				break
			}
		}
	}
	if whereType == QueryOperationAND {
		q.where = append(q.where, nodes...)
	} else {
		q.match(whereType, nodes...)
	}
}

func (q *Query) Where(format any, cons ...any) {
	switch k := format.(type) {
	case string:
		if IsQueryFormat(k) {
			q.formClause(k, cons)
		} else if l := len(cons); l == 0 {
			q.formPrimary(k)
		} else if l == 1 {
			q.formString(k, cons[0])
		} else {
			q.formString(k, cons)
		}
	case Filter:
		if q.filter == nil {
			q.filter = k
		} else {
			q.filter.Merge(k)
		}
	case map[string]any:
		q.fromMap(k)
	case bson.M:
		q.fromMap(k)
	default:
		if len(cons) == 0 {
			q.formPrimary(k)
		} else {
			logger.Alert("Query where format type unknown:%v", format)
		}
	}
}

func parseWherePair(pair string, w string, v interface{}) *Node {
	//fmt.Printf("parseWherePair %v---%v ---%v\n", pair, w, v)
	arr := strings.Split(pair, w)
	//fmt.Printf("parseWherePair ARR: %v \n", arr)
	if len(arr) != 2 {
		return nil
	}
	node := &Node{}
	node.t = QueryOperationPrefix + whereConditionMongo[w]
	node.k = strings.Trim(arr[0], sqlConditionSplit)

	var r interface{}
	r = strings.Trim(arr[1], sqlConditionSplit)
	if r == "?" {
		r = v
	} else {
		r = formatWhereValue(r)
	}

	node.v = r
	//fmt.Printf("parseWherePair node: %+v \n", node)
	return node
}

func formatWhereValue(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	for t, f := range formatWhereTypes {
		if strings.HasPrefix(s, t) {
			return f(t, s)
		}
	}
	return v
}
