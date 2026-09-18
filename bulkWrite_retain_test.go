package cosmo

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// 本文件钉住 BulkWrite.Submit 的「部分成功清理」语义:
// unordered 批量逐条错误按 Index 剔除已生效条目,只保留失败的供下次重试;
// 无法定位条目的错误(断连/写关注失败/非异常错误)全量保留。

func bwInsModel(id int) *mongo.InsertOneModel {
	m := mongo.NewInsertOneModel()
	m.SetDocument(map[string]any{"_id": id})
	return m
}

// bwModelID 从 InsertOneModel 取回 _id,用于断言剩余条目
func bwModelID(m mongo.WriteModel) int {
	doc := m.(*mongo.InsertOneModel).Document.(map[string]any)
	return doc["_id"].(int)
}

// 🔴 部分成功:逐条错误按 Index 剔除已生效条目,只保留失败的
func TestBulkWriteRetainFailuresPartial(t *testing.T) {
	bw := &BulkWrite{models: []mongo.WriteModel{
		bwInsModel(0), bwInsModel(1), bwInsModel(2), bwInsModel(3),
	}}
	err := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 1, Code: 11000, Message: "E11000 duplicate key"}},
		{WriteError: mongo.WriteError{Index: 3, Code: 121, Message: "document validation failure"}},
	}}
	bw.retainFailures(err)

	if len(bw.models) != 2 {
		t.Fatalf("应只保留失败条目,实际 %d 个", len(bw.models))
	}
	for i, want := range []int{0, 2} {
		if got := bwModelID(bw.models[i]); got != want {
			t.Fatalf("剩余条目[%d] 应为 _id=%d,实际 %d", i, want, got)
		}
	}
}

// 🔴 重发撞重复主键的收敛场景:上次断连全量保留,重发时已写入的插入逐条报 E11000,
// 全部剔除后队列清空,不会永久卡死
func TestBulkWriteRetainFailuresConverge(t *testing.T) {
	bw := &BulkWrite{models: []mongo.WriteModel{bwInsModel(0), bwInsModel(1)}}
	err := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 0, Code: 11000, Message: "E11000 duplicate key"}},
		{WriteError: mongo.WriteError{Index: 1, Code: 11000, Message: "E11000 duplicate key"}},
	}}
	bw.retainFailures(err)
	if len(bw.models) != 0 {
		t.Fatalf("全部逐条失败(重复主键)应清空队列,实际剩 %d 个", len(bw.models))
	}
}

// 无法定位条目的错误:非异常错误 / 仅写关注失败,全量保留
func TestBulkWriteRetainFailuresKeepAll(t *testing.T) {
	//非 BulkWriteException(断连等)
	bw := &BulkWrite{models: []mongo.WriteModel{bwInsModel(0), bwInsModel(1)}}
	bw.retainFailures(errors.New("connection reset by peer"))
	if len(bw.models) != 2 {
		t.Fatalf("断连错误应全量保留,实际剩 %d 个", len(bw.models))
	}

	//仅写关注失败:整批已生效但确认不足,保留重发($set/$unset/delete 幂等)
	bw = &BulkWrite{models: []mongo.WriteModel{bwInsModel(0), bwInsModel(1)}}
	bw.retainFailures(mongo.BulkWriteException{
		WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "waiting for replication timed out"},
	})
	if len(bw.models) != 2 {
		t.Fatalf("仅写关注失败应全量保留,实际剩 %d 个", len(bw.models))
	}
}

// resolveOrdered:还原 opts 的 Ordered 设置;未设置时为 mongo 默认 true
func TestBulkWriteResolveOrdered(t *testing.T) {
	bw := &BulkWrite{}
	if !bw.resolveOrdered() {
		t.Fatal("未设置 Ordered 时应为 mongo 默认 true")
	}
	bw.opts = append(bw.opts, options.BulkWrite().SetOrdered(false))
	if bw.resolveOrdered() {
		t.Fatal("SetOrdered(false) 应解析为 unordered")
	}
	bw.opts = append(bw.opts, options.BulkWrite().SetOrdered(true))
	if !bw.resolveOrdered() {
		t.Fatal("多个 Lister 应按顺序覆盖,最终为 true")
	}
}
