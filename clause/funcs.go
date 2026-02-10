package clause

import "go.mongodb.org/mongo-driver/v2/bson"

// Multiple 判断是批量操作还是单个文档操作
// 通过检查查询条件中主键字段的值类型来判断：
// - 如果主键字段不存在，默认返回true（批量操作）
// - 如果主键字段值是map或bson.M类型，返回true（批量操作，如使用$in等条件）
// - 否则返回false（单个文档操作）
//
// 参数 query: 查询条件Filter
// 返回值: true表示批量操作，false表示单个文档操作
func Multiple(query Filter) bool {
	v, ok := query[MongoPrimaryName]
	if !ok {
		return true
	}
	switch v.(type) {
	case map[string]interface{}, bson.M:
		return true
	}
	return false
}
