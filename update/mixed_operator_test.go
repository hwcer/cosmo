package update

import (
	"testing"

	"github.com/hwcer/cosgo/schema"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type mixedRole struct {
	Name string `bson:"name"`
	Lv   int32  `bson:"lv"`
}

func mustParseSchema(t *testing.T, model any) *schema.Schema {
	t.Helper()
	sch, err := schema.Parse(model)
	if err != nil {
		t.Fatalf("schema parse: %v", err)
	}
	return sch
}

// 🔴 P1 回归:文档承诺的 map/bson.M 混合操作符写法必须生效
// 旧实现两个洞:bson.M 命名类型接不住落 default 整批塞 $set;
// $inc 键被当字段名 Transform 报错
func TestParseMapMixedOperators(t *testing.T) {
	sch := mustParseSchema(t, &mixedRole{})

	//bson.M 混合:$set 隐式 + $inc 显式
	u, err := parseMap(bson.M{"Name": "tom", "$inc": bson.M{"Lv": 1}}, sch)
	if err != nil {
		t.Fatalf("bson.M 混合操作符不应报错:%v", err)
	}
	if v, ok := u["$set"]["name"]; !ok || v != "tom" {
		t.Fatalf("Name 应换名进 $set:%v", u["$set"])
	}
	if v, ok := u["$inc"]["lv"]; !ok || v != 1 {
		t.Fatalf("Lv 应换名进 $inc:%v", u["$inc"])
	}

	//map[string]any 同款
	u2, err := parseMap(map[string]any{"Name": "tom", "$inc": map[string]any{"Lv": 2}}, sch)
	if err != nil {
		t.Fatalf("map 混合操作符不应报错:%v", err)
	}
	if v, ok := u2["$inc"]["lv"]; !ok || v != 2 {
		t.Fatalf("map 形态 $inc 应生效:%v", u2["$inc"])
	}

	//普通 map 仍走 $set
	u3, err := parseMap(map[string]any{"Name": "tom"}, sch)
	if err != nil {
		t.Fatalf("普通 map 不应报错:%v", err)
	}
	if v, ok := u3["$set"]["name"]; !ok || v != "tom" {
		t.Fatalf("普通键应进 $set:%v", u3["$set"])
	}
}

// 🔴 回归:显式 $set 块与普通键共存时必须合并——
// 旧实现尾部无条件赋值把循环刚建好的 $set 块整块覆盖,bson.M{"Name":"x","$set":bson.M{"Lv":7}}
// 里 Lv:7 静默丢失(写库"成功"但字段没变)
func TestParseMapExplicitSetMerge(t *testing.T) {
	sch := mustParseSchema(t, &mixedRole{})
	u, err := parseMap(bson.M{"Name": "x", "$set": bson.M{"Lv": 7}}, sch)
	if err != nil {
		t.Fatalf("显式 $set 混写不应报错:%v", err)
	}
	if v, ok := u["$set"]["name"]; !ok || v != "x" {
		t.Fatalf("普通键 Name 应进 $set:%v", u["$set"])
	}
	if v, ok := u["$set"]["lv"]; !ok || v != 7 {
		t.Fatalf("显式 $set 块的 Lv 不得被覆盖丢失:%v", u["$set"])
	}
}
