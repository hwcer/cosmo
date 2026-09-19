package clause

import "encoding/json"
import "testing"

// 🔴 回归:同一 Query 重复 Build 必须幂等——旧实现回写 q.filter,
// 第二次 Build 把 where 重放到上次结果上,$in 每次翻倍
func TestBuildIdempotent(t *testing.T) {
	q := New()
	q.Where("name = ? AND age > ?", "tom", 18)

	first, _ := json.Marshal(q.Build(nil))
	second, _ := json.Marshal(q.Build(nil))
	if string(first) != string(second) {
		t.Fatalf("重复 Build 结果漂移:\nfirst :%s\nsecond:%s", first, second)
	}
}
