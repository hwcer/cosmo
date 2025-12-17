package clause

import "github.com/hwcer/cosgo/schema"

// Build 生成MongoDB查询条件，支持模型字段映射和复杂条件构建
// 参数 model: 可选的模型schema，用于字段名映射（结构体字段名到数据库字段名）
// 返回值: 构建完成的Filter查询条件
//
// 使用示例：
// query := clause.New().Eq("name", "test").Gt("age", 18)
// filter := query.Build(userSchema)
// // 结果: { "name": "test", "age": { "$gt": 18 } }
func (q *Query) Build(model *schema.Schema) Filter {
	filter := make(Filter)
	if q.filter != nil {
		filter.Merge(q.filter)
	}
	if len(q.where) == 0 {
		return filter
	}

	for _, node := range q.where {
		q.build(model, filter, node)
	}
	for _, t := range complexCondition {
		for _, node := range q.complex[t] {
			v := make(Filter)
			q.build(model, v, node)
			filter.Match(t, v)
		}
	}
	q.filter = filter
	return filter
}

// build 内部辅助函数，用于构建单个条件节点
// 参数 model: 模型schema，用于字段名映射
// 参数 filter: 目标Filter对象，用于存储构建的条件
// 参数 node: 条件节点，包含操作类型、字段名和值
func (q *Query) build(model *schema.Schema, filter Filter, node *Node) {
	k := node.k
	if model != nil {
		if filed := model.LookUpField(node.k); filed != nil {
			k = filed.DBName()
		}
	}
	if node.t == QueryOperationPrefix {
		filter.Eq(k, node.v)
	} else {
		filter.Any(node.t, k, node.v)
	}
}
