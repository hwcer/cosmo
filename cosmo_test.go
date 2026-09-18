package cosmo

import (
	"strconv"
	"testing"
	"time"

	"github.com/hwcer/cosmo/clause"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Role struct {
	Id   string `bson:"_id"`
	Name string `bson:"name"`
	Lv   int64  `bson:"lv"`
	Exp  int64  `bson:"exp"`
}

func TestCosmo(t *testing.T) {
	db := New()
	var err error
	if err = db.Start("hwc#1", "127.0.0.1:27017"); err != nil {
		t.Logf("%v", err)
		return
	}
	id := strconv.Itoa(int(time.Now().Unix()))
	role := &Role{Id: id, Name: "test"}
	if tx := db.Create(role); tx.Error != nil {
		t.Logf("Create error:%v", tx.Error)
		return
	}
	//if err := db.AutoMigrator(&Role{}); err != nil {
	//	t.Logf("AutoMigrator Error:%v", err)
	//}

	t.Logf("================Find Many=====================")
	var roles []*Role
	paging := &Paging{}
	paging.Init(100)
	paging.Page = 1
	tx := db.Table("role").Omit("_id").Page(paging, 2).Order("_id", -1).Find(&roles)
	if db.Error != nil {
		t.Logf("Find error:%v", tx.Error)
	} else {
		t.Logf("RowsAffected:%v", tx.RowsAffected)
		for _, v := range roles {
			t.Logf("role:%+v", v)
		}
	}

	t.Logf("==================Update===================")
	update := bson.M{"Name": "changed name"}
	update["$inc"] = bson.M{"lv": 1, "exp": 100}
	tx = db.Model(role).Update(update)
	if db.Error != nil {
		t.Logf("%v", db.Error)
	} else {
		t.Logf("RowsAffected:%v,role:%+v", tx.RowsAffected, role)
	}

	t.Logf("==================Find One===================")
	tx = db.Find(role)
	if db.Error != nil {
		t.Logf("%v", db.Error)
	} else {
		t.Logf("RowsAffected:%v,role:%+v", tx.RowsAffected, role)
	}
	t.Logf("=================count====================")
	var count int
	tx = db.Model(&Role{}).Count(&count)
	if tx.Error != nil {
		t.Logf("%v", tx.Error)
	} else {
		t.Logf("count:%v", count)
	}
	t.Logf("=================delete====================")
	tx = db.Model(&Role{}).Delete(role)
	if tx.Error != nil {
		t.Logf("%v", tx.Error)
	} else {
		t.Logf("delete:%v", tx.RowsAffected)
	}
}

// +++[alexjin][2026-09-18]
// TestMatchPipeline 纯构建测试（不依赖mongo）：$match拼接顺序与调用方切片不可变
func TestMatchPipeline(t *testing.T) {
	q := New()
	q = q.Where("guild = ?", "g1") // Where返回克隆体,必须接住
	filter := q.stmt.Clause.Build(nil)
	orig := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "sum", Value: bson.D{{Key: "$sum", Value: "$power"}}},
		}}},
	}
	got := matchPipeline(filter, orig)
	if len(got) != 2 {
		t.Fatalf("拼接后应为2阶段,实际 %d", len(got))
	}
	if got[0][0].Key != "$match" {
		t.Errorf("第0阶段应为$match,实际 %v", got[0][0].Key)
	}
	if got[1][0].Key != orig[0][0].Key {
		t.Errorf("原管道阶段应原样后移,实际 %v", got[1][0].Key)
	}
	// 调用方切片不可变
	if len(orig) != 1 || cap(orig) != 1 {
		t.Errorf("调用方管道被修改:len=%d cap=%d", len(orig), cap(orig))
	}
	// 空filter原样返回
	if got := matchPipeline(clause.Filter{}, orig); len(got) != len(orig) {
		t.Errorf("空filter应原样返回,实际 %d 阶段", len(got))
	}
}

// TestAggregate 聚合查询端到端（连本机mongo，不可达软跳过）
func TestAggregate(t *testing.T) {
	db := New()
	if err := db.Start("hwc#1", "127.0.0.1:27017"); err != nil {
		t.Logf("%v", err)
		return
	}
	coll := "aggregate_test"
	docs := []any{
		bson.M{"_id": "agg1", "guild": "g1", "power": int64(100)},
		bson.M{"_id": "agg2", "guild": "g1", "power": int64(250)},
		bson.M{"_id": "agg3", "guild": "g2", "power": int64(999)},
	}
	if tx := db.Table(coll).Create(docs); tx.Err() != nil {
		t.Logf("seed error:%v", tx.Err())
		return
	}
	defer func() {
		db.Table(coll).Where("_id IN ?", []string{"agg1", "agg2", "agg3"}).Delete()
	}()

	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "sum", Value: bson.D{{Key: "$sum", Value: "$power"}}},
		}}},
	}

	// 1) Where经Model schema翻译为$match
	type sumRow struct {
		Sum int64 `bson:"sum"`
	}
	type seed struct {
		Id    string `bson:"_id"`
		Guild string `bson:"guild"`
		Power int64  `bson:"power"`
	}

	var rows []sumRow
	tx := db.Model(&seed{}).Table(coll).Where("guild = ?", "g1").Aggregate(&rows, pipeline)
	if tx.Err() != nil {
		t.Fatalf("aggregate error:%v", tx.Err())
	}
	if len(rows) != 1 || rows[0].Sum != 350 {
		t.Fatalf("g1总战力应为350,实际 %+v", rows)
	}
	if tx.RowsAffected != 1 {
		t.Errorf("RowsAffected应为1,实际 %d", tx.RowsAffected)
	}

	// 2) conds 传参形式
	var rows2 []sumRow
	if tx := db.Model(&seed{}).Table(coll).Aggregate(&rows2, pipeline, "guild = ?", "g2"); tx.Err() != nil {
		t.Fatalf("aggregate(conds) error:%v", tx.Err())
	}
	if len(rows2) != 1 || rows2[0].Sum != 999 {
		t.Fatalf("g2总战力应为999,实际 %+v", rows2)
	}

	// 3) 无匹配文档：$group不产出,dest为空切片且无错误
	var rows3 []sumRow
	tx = db.Model(&seed{}).Table(coll).Where("guild = ?", "nope").Aggregate(&rows3, pipeline)
	if tx.Err() != nil {
		t.Fatalf("aggregate(nomatch) error:%v", tx.Err())
	}
	if len(rows3) != 0 || tx.RowsAffected != 0 {
		t.Errorf("无匹配应为空结果,RowsAffected=%d rows=%d", tx.RowsAffected, len(rows3))
	}

	// 4) dest非指针切片：明确报错
	rows4 := []sumRow{}
	if tx := db.Model(&seed{}).Table(coll).Aggregate(rows4, pipeline); tx.Err() == nil {
		t.Fatal("dest非指针切片应报错")
	}

	// 5) 缺 Model/Table：明确报错
	if tx := db.Aggregate(&rows, pipeline); tx.Err() == nil {
		t.Fatal("缺 Model/Table 应报错")
	}
}

//---[alexjin][2026-09-18]
