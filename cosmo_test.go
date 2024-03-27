package cosmo

import (
	"github.com/hwcer/cosgo/values"
	"go.mongodb.org/mongo-driver/bson"
	"strconv"
	"testing"
	"time"
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
	paging := &values.Paging{}
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
	tx = db.Delete(role)
	if tx.Error != nil {
		t.Logf("%v", tx.Error)
	} else {
		t.Logf("delete:%v", tx.RowsAffected)
	}
}
