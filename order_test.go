package cosmo

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// 🔴 P1 回归:复合排序必须按调用顺序生成——旧实现 orders 用 map 存储,
// range 迭代序随机,实测 3 键 30 轮出现 3 种排列,榜单/分页切片内容在
// 多次调用间漂移
func TestOrderPreservesCallSequence(t *testing.T) {
	db := &DB{clone: true}
	db.stmt = NewStatement(db)

	db.Order("lv", -1).Order("exp", -1).Order("name", 1)
	order := db.stmt.Order()
	if len(order) != 3 {
		t.Fatalf("应有 3 个排序键,实际 %d", len(order))
	}
	want := []bson.E{
		{Key: "lv", Value: -1},
		{Key: "exp", Value: -1},
		{Key: "name", Value: 1},
	}
	for i, e := range want {
		if order[i] != e {
			t.Fatalf("第 %d 位应为 %v,实际 %v(顺序漂移=随机排序)", i, e, order[i])
		}
	}
}

// 同键重复 Order 覆盖方向且保持原位置(旧 map 语义)
func TestOrderSameKeyOverridesDirection(t *testing.T) {
	db := &DB{clone: true}
	db.stmt = NewStatement(db)

	db.Order("lv", -1).Order("name", 1).Order("lv", 1)
	order := db.stmt.Order()
	if len(order) != 2 {
		t.Fatalf("同键覆盖后应剩 2 个键,实际 %d", len(order))
	}
	if order[0].Key != "lv" || order[0].Value != 1 {
		t.Fatalf("lv 应保持原位且方向覆盖为 1,实际 %v", order[0])
	}
}
