package cosmo

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// 🔴 回归:批量路径必须接住 Where 解析错误与空过滤器——
// 坏条件退化为空 filter 的 DeleteMany 是全集合删除,UpdateOne 会静默更新任意一条。
// 单条路径(cmdUpdate/cmdDelete)一直有拦截,批量路径曾完全裸奔(本测试钉住补齐后的行为)

// 解析失败的条件:进入 tx.Error,不生成任何模型,Submit 在触达 mongo 前返回错误
func TestBulkWriteBadWhereBlocked(t *testing.T) {
	db := New()
	bw := db.BulkWrite(&Role{})
	bw.Update(bson.M{"name": "x"}, "a = b = c")
	if bw.tx.Error == nil {
		t.Fatal("解析失败的条件应进入 tx.Error,不得生成批量模型")
	}
	if bw.Size() != 0 {
		t.Fatalf("不应生成任何模型,实际 %d 个", bw.Size())
	}
	if err := bw.Submit(); err == nil {
		t.Fatal("Submit 应在触达 mongo 前返回错误")
	}
}

// 空过滤器:Delete(bson.M{}) 不得产出全集合 DeleteMany
func TestBulkWriteEmptyFilterBlocked(t *testing.T) {
	db := New()
	bw := db.BulkWrite(&Role{})
	bw.Delete(bson.M{})
	if bw.tx.Error == nil {
		t.Fatal("空过滤器应被 ErrMissingWhereClause 拦截")
	}
	if bw.Size() != 0 {
		t.Fatalf("不应生成任何模型,实际 %d 个", bw.Size())
	}
}

// BulkWrite8 同口径:解析失败进 bw8.Error,Submit 在触达 mongo 前返回错误
func TestBulkWrite8BadWhereBlocked(t *testing.T) {
	db := New()
	bw8 := db.BulkWrite8()
	bw8.Update(&Role{}, bson.M{"name": "x"}, "a = b = c")
	if bw8.Error == nil {
		t.Fatal("解析失败的条件应进入 bw8.Error")
	}
	if bw8.Size() != 0 {
		t.Fatalf("不应生成任何操作,实际 %d 个", bw8.Size())
	}
	if err := bw8.Submit(); err == nil {
		t.Fatal("Submit 应在触达 mongo 前返回错误")
	}
}

func TestBulkWrite8EmptyFilterBlocked(t *testing.T) {
	db := New()
	bw8 := db.BulkWrite8()
	bw8.Delete(&Role{}, bson.M{})
	if bw8.Error == nil {
		t.Fatal("空过滤器应被 ErrMissingWhereClause 拦截")
	}
	if bw8.Size() != 0 {
		t.Fatalf("不应生成任何操作,实际 %d 个", bw8.Size())
	}
}

// resolveOrdered:未显式设置 Ordered 时兜底 ordered(保守侧,全量保留),
// 与驱动 v2 ClientBulkWrite 的默认一致
func TestBulkWrite8ResolveOrderedDefault(t *testing.T) {
	bw8 := &BulkWrite8{}
	if !bw8.resolveOrdered() {
		t.Fatal("nil Ordered 应兜底到 ordered")
	}
}
