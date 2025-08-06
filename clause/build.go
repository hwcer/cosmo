package clause

import "github.com/hwcer/cosgo/schema"

// Build 生成mongo查询条件
func (q *Query) Build(model *schema.Schema) Filter {
	filter := make(Filter)
	if q.filter != nil {
		filter.Merge(q.filter)
	}
	for _, node := range q.where {
		build(model, filter, node)
	}
	for _, t := range complexCondition {
		for _, node := range q.complex[t] {
			v := make(Filter)
			build(model, v, node)
			filter.Match(t, v)
		}
	}
	return filter
}

func build(model *schema.Schema, filter Filter, node *Node) {
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
