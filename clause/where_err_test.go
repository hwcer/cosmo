package clause

import (
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
