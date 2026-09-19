package clause

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"strings"
	"testing"
)

// 🔴 T4 回归:SQL 风格 Where 的解析失败必须进 Query.Err——
// 旧实现静默丢弃解析不了的条件,删除/更新的匹配范围被放大(少一个条件=多删多改一片)

// 拆分形态错误(多段):parseWherePair 返回 nil,必须报错而非静默丢弃。
// 注:带引号字面量(name = 'myname' → 值含引号)属值格式问题,不在此路径,
// 由 P1-5 字面量陷阱单独处理
func TestWhereParseFailureSurfaces(t *testing.T) {
	q := New()
	q.Where("a = b = c") //以 = 拆出 3 段,parseWherePair 拒绝
	if q.Err == nil {
		t.Fatal("拆分失败的条件应产生解析错误(旧实现静默丢弃)")
	}
}

// 片段命中操作符但拆分后缺值/多段:parseWherePair 返回 nil,必须报错
func TestWhereMalformedPairSurfaces(t *testing.T) {
	q := New()
	// "a = b = c" 以 = 拆出 3 段,parseWherePair 拒绝 → Err
	q.Where("a = b = c")
	if q.Err == nil {
		t.Fatal("拆分失败的条件应产生解析错误(旧实现静默丢弃)")
	}
}

// 正常条件不受影响
func TestWhereValidPairNoError(t *testing.T) {
	q := New()
	q.Where("name = ?", "tom")
	if q.Err != nil {
		t.Fatalf("合法条件不应报错:%v", q.Err)
	}
	f := q.Build(nil)
	if _, ok := f["name"]; !ok {
		t.Fatalf("name 条件应存在:%v", f)
	}
}

// 混合形态:失败片段上抛,合法片段照常生成(执行前由上层拦截,不回滚已生成条件)
func TestWherePartialFailureKeepsError(t *testing.T) {
	q := New()
	q.Where("a = b = c AND name = ?", "tom")
	if q.Err == nil {
		t.Fatal("混合条件中失败片段应报错")
	}
	if len(q.where) != 1 {
		t.Fatalf("合法片段应照常生成条件节点,实际 %d 个", len(q.where))
	}
}

// 🔴 P1-5 回归:字面量一律拒绝——字符串写法里无法得知业务类型,旧实现原样当
// 字符串产出 {"lv":{"$gt":"10"}} 永不匹配的静默空查询(BSON 类型序数字<字符串,
// MongoDB 不做类型转换)。正确写法:占位符+类型化参数,或显式前缀 int(10)

// 裸数字字面量:拒绝
func TestWhereBareNumberRejected(t *testing.T) {
	q := New()
	q.Where("lv > 10")
	if q.Err == nil {
		t.Fatal(`裸数字字面量应被拒绝(旧实现产出 {"lv":{"$gt":"10"}} 永不匹配)`)
	}
	if !strings.Contains(q.Err.Error(), "use ? placeholder") {
		t.Fatalf("错误信息应引导占位符用法:%v", q.Err)
	}
}

// SQL 习惯的引号字符串:拒绝(引号会成为值的一部分,查询的其实是 "'tom'")
func TestWhereQuotedStringRejected(t *testing.T) {
	q := New()
	q.Where("name = 'tom'")
	if q.Err == nil {
		t.Fatal("引号字面量应被拒绝(旧实现产出 {name:'tom'} 查的是带引号的四个字符)")
	}
}

// 显式类型前缀是字面量的唯一合法形态:正常生成
func TestWhereTypedPrefixAllowed(t *testing.T) {
	q := New()
	q.Where("lv > int(10)")
	if q.Err != nil {
		t.Fatalf("显式前缀字面量应放行:%v", q.Err)
	}
	f := q.Build(nil)
	gt, ok := f["lv"].(bson.M)
	if !ok {
		t.Fatalf("lv 条件应为 bson.M:%#v", f["lv"])
	}
	if gt["$gt"] != int(10) {
		t.Fatalf("int(10) 应转成数字 10:%#v", gt)
	}
}
