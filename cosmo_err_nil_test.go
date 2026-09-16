package cosmo

import (
	"fmt"
	"testing"

)

// 🔴 回归：DB.Error 的类型是 *values.Message（可实现 error 接口），空的
// *values.Message（nil 指针或 "<nil>" 文案的非 nil 对象）都不能作为错误
// 存进 DB.Error——装进 error 接口就是 typed-nil（err != nil 判真、%v 打
// <nil>、Error() 方法 panic）。2026-09-16 实际发生过：yyds players/loading
// 把残留的空 Message 当错误上抛，启动 FATL「原因: <nil>」。
func TestErrorfNilClears(t *testing.T) {
	db := &DB{}
	// nil 入参 = 清空语义，不产出 "<nil>" 文案的垃圾 Message
	// 先种一个已存在的错误，验证 Errorf(nil) **跳过不清空**——不能吞错误
	db.Errorf("keep me")
	db.Errorf(nil) // nil 入参必须被跳过
	if db.Error == nil || db.Error.Error() != "keep me" {
		t.Fatal("Errorf(nil) 不得清空/覆盖已存在的 Error（吞错误）")
	}

	// ⚠️ 语言级事实（顺钉住，防止误判）：字段为 nil 指针时外部装箱依然是
	// typed-nil（err != nil 判真、%v 打 <nil>、Error() panic）——Go 接口的固有
	// 行为，框架拦不掉装箱。读取方必须走 Err() 或显式判空（updater 范式；
	// yyds players/loading 1777584 即按此修复）。
	db2 := &DB{}
	db2.Errorf(nil) // 从未有错误：nil 入参跳过后 Error 仍是 nil 指针
	var e error = db2.Error
	if e == nil {
		t.Fatal("前提不成立：nil *Message 装箱后 err==nil，typed-nil 未复现")
	}
	if fmt.Sprint(e) != "<nil>" {
		t.Fatalf("typed-nil 的 %%v 应为 <nil>, got %q", fmt.Sprint(e))
	}

	// 真错误照常写入且内容完整（非 nil 才处理）
	db3 := &DB{}
	db3.Errorf("boom %d", 7)
	if db3.Error == nil || db3.Error.Error() != "boom 7" {
		t.Fatal("真错误应原样写入")
	}
	// error 接口入参（非 nil）同样写入
	db3.Errorf(fmt.Errorf("wrap %d", 8))
	if db3.Error == nil || db3.Error.Error() != "wrap 8" {
		t.Fatal("error 入参应原样写入")
	}

	// Err() 出口：nil 时返回真 nil（读取侧防 typed-nil 的配套）
	db4 := &DB{}
	if err := db4.Err(); err != nil {
		t.Fatal("Err() 在 Error 为 nil 时应返回真 nil")
	}
	if err := db3.Err(); err == nil || err.Error() != "wrap 8" {
		t.Fatal("Err() 应返回带内容的 error")
	}
}
