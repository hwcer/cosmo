package cosmo

import (
	"fmt"
	"testing"

	"github.com/hwcer/cosgo/values"
)

// 🔴 回归：DB.Error 的类型是 *values.Message（可实现 error 接口），空的
// *values.Message（nil 指针或 "<nil>" 文案的非 nil 对象）都不能作为错误
// 存进 DB.Error——装进 error 接口就是 typed-nil（err != nil 判真、%v 打
// <nil>、Error() 方法 panic）。2026-09-16 实际发生过：yyds players/loading
// 把残留的空 Message 当错误上抛，启动 FATL「原因: <nil>」。
func TestErrorfNilClears(t *testing.T) {
	db := &DB{}
	// nil 入参 = 清空语义，不产出 "<nil>" 文案的垃圾 Message
	db.Errorf(nil)
	if db.Error != nil {
		t.Fatalf("Errorf(nil) 后 DB.Error 应为 nil（typed-nil 源头）")
	}
	// ⚠️ 语言级事实（顺钉住，防止误判）：字段为 nil 指针时外部装箱依然是
	// typed-nil（err != nil 判真）——这是 Go 接口的固有行为，框架拦不掉。
	// 所以读取 tx.Error 的人必须走 Err() 或显式判空（updater 范式，
	// yyds players/loading 1777584 即按此修复）。
	var e error = db.Error
	if e == nil {
		t.Fatal("前提不成立：nil *Message 装箱后 err==nil，typed-nil 未复现")
	}
	if fmt.Sprint(e) != "<nil>" {
		t.Fatalf("typed-nil 的 %%v 应为 <nil>, got %q", fmt.Sprint(e))
	}
	// 空指针同样不得入内：NormalizeError(nil) = nil
	db.Error = NormalizeError(nil)
	if db.Error != nil {
		t.Fatal("NormalizeError(nil) 应返回 nil")
	}
	// 真错误照常写入且内容完整
	db.Errorf("boom %d", 7)
	if db.Error == nil || db.Error.Error() != "boom 7" {
		t.Fatalf("真错误应原样写入")
	}
	// Err() 出口：nil 时返回真 nil（读取侧防 typed-nil 的配套）
	db2 := &DB{}
	if err := db2.Err(); err != nil {
		t.Fatalf("Err() 在 Error 为 nil 时应返回真 nil")
	}
	db3 := &DB{}
	db3.Errorf("x")
	if err := db3.Err(); err == nil || err.Error() != "x" {
		t.Fatalf("Err() 应返回带内容的 error")
	}
	// 兜底断言：任何路径写入后都不允许出现 "<nil>" 文案的空错误
	if db.Error != nil && db.Error.Error() == fmt.Sprint(nil) {
		t.Fatal("不允许 <nil> 文案错误")
	}
	_ = values.Message{}
}
