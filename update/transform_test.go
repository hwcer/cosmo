package update

import (
	"strings"
	"testing"

	"github.com/hwcer/cosgo/schema"
)

// 仿 pb.go 生成物：json 标签 PascalCase、无 bson 标签（DBName 回落成小写）。
// 这正是 updater 交过来的 key 与落库名不相等的形态。
type xfLeaf struct {
	Lv int32 `json:"Lv"`
}

type xfRole struct {
	Uid        string            `bson:"_id" json:"uid"`
	BreakLv    int32             `json:"BreakLv"`
	Goods      map[int32]int64   `json:"goods"`
	SoulRelics map[int32]*xfLeaf `json:"SoulRelics"`
	Timers     map[int64]*xfLeaf `json:"Timers"`
}

func xfSchema(t *testing.T) *schema.Schema {
	t.Helper()
	sch, err := schema.Parse(&xfRole{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return sch
}

// 🔴 updater 内部统一发 json 名，落库名必须在这里换出来。
//
// 旧实现有两个静默的洞：含 "." 的 key 原样下发（往库里插野字段），
// 单段 key 查不到时连键带值直接消失。
func TestTransformRenamesToDBName(t *testing.T) {
	sch := xfSchema(t)

	u := New()
	u.Set("BreakLv", 3)          //整字段：json 名 → 落库名
	u.Set("SoulRelics.1", "x")   //带点：根字段换名，map 键原样
	u.Set("SoulRelics.1.Lv", 9)  //穿过 map 键继续下钻
	u.Inc("Goods.10001", 5)      //$inc 同样要换
	u.Unset("Timers.4294967297") //$unset 同样要换
	u.SetOnInsert("Uid", "u1")   //有 bson 标签时以标签为准

	got, err := u.Transform(sch)
	if err != nil {
		t.Fatalf("Transform 报错: %v", err)
	}

	want := map[string]map[string]any{
		UpdateTypeSet:         {"breaklv": 3, "soulrelics.1": "x", "soulrelics.1.lv": 9},
		UpdateTypeInc:         {"goods.10001": 5},
		UpdateTypeUnset:       {"timers.4294967297": 1},
		UpdateTypeSetOnInsert: {"_id": "u1"},
	}
	for op, fields := range want {
		m, ok := got[op]
		if !ok {
			t.Errorf("%s 缺失", op)
			continue
		}
		if len(m) != len(fields) {
			t.Errorf("%s 字段数不符: %v", op, m)
		}
		for k := range fields {
			if _, ok = m[k]; !ok {
				t.Errorf("%s 期望含落库名 %q，实际 %v", op, k, m)
			}
		}
	}
}

// 解析不出来的 key 必须报错。
//
// 旧实现是 `else if field := ...; field != nil {}` —— 没有 else 分支，
// 查不到的键连同值直接消失，写入“成功”但数据没了。
func TestTransformRejectsUnknownField(t *testing.T) {
	sch := xfSchema(t)

	for _, k := range []string{"NoSuchField", "NoSuchField.1", "SoulRelics.1.Nope", "BreakLv.1"} {
		u := New()
		u.Set(k, 1)
		if _, err := u.Transform(sch); err == nil {
			t.Errorf("Transform 对 %q 应报错，而不是静默丢弃", k)
		} else if !strings.Contains(err.Error(), "$set") {
			t.Errorf("错误信息应带上操作类型，实际: %v", err)
		}
	}
}

// 投影用同一套换名。带点的 key 若不换，会投影到一个不存在的字段，
// 那段数据静默读不出来。
func TestProjectionRenamesToDBName(t *testing.T) {
	sch := xfSchema(t)

	s := &Selector{}
	s.Select("BreakLv", "SoulRelics.1", "SoulRelics.1.Lv", "Uid")
	p := s.Projection(sch)

	for _, k := range []string{"breaklv", "soulrelics.1", "soulrelics.1.lv", "_id"} {
		if _, ok := p[k]; !ok {
			t.Errorf("投影应含落库名 %q，实际 %v", k, p)
		}
	}
}

// 解析不出来的投影 key 退回原样（少读一个字段，不该让整次查询失败）
func TestProjectionKeepsUnknownKey(t *testing.T) {
	sch := xfSchema(t)

	s := &Selector{}
	s.Select("NoSuchField")
	p := s.Projection(sch)
	if _, ok := p["NoSuchField"]; !ok {
		t.Fatalf("未知投影字段应原样保留，实际 %v", p)
	}
}
