package update

import (
	"encoding/json"
	"strings"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// 更新操作类型常量定义
const (
	UpdateTypeSet         = "$set"         // 设置字段值
	UpdateTypeInc         = "$inc"         // 增加/减少字段值
	UpdateTypeUnset       = "$unset"       // 删除字段
	UpdateTypeSetOnInsert = "$setOnInsert" // 仅在插入时设置字段值
)

// projectionField 用于生成投影的更新操作类型
var projectionField = []string{UpdateTypeSet, UpdateTypeInc}

// New 创建一个新的Update实例
// 返回值: 空的Update实例
func New() Update {
	return make(Update)
}

// NewFromMap 从map创建Update实例
// 参数 v: 包含字段名和值的map
// 返回值: 设置了字段的Update实例
func NewFromMap(v map[string]any) Update {
	r := make(Update)
	r.Save(v)
	return r
}

// Update 定义MongoDB更新操作
// 类型为map[string]bson.M，键为更新操作类型（如$set, $inc），值为对应的字段和值

type Update map[string]bson.M

// Has 检查更新操作中是否包含指定的字段
// 参数 opt: 更新操作类型（如$set）
// 参数 filed: 字段名
// 返回值: true表示包含该字段
func (u Update) Has(opt string, filed string) bool {
	if vs, ok := u[opt]; !ok {
		return false
	} else {
		_, ok = vs[filed]
		return ok
	}
}

// Get 获取更新操作中指定字段的值
// 参数 opt: 更新操作类型（如$set）
// 参数 k: 字段名
// 返回值: 字段值和是否存在
func (u Update) Get(opt string, k string) (v any, ok bool) {
	var vs bson.M
	if vs, ok = u[opt]; ok {
		v, ok = vs[k]
	}
	return
}

// Remove 从更新操作中删除指定字段
// 参数 opt: 更新操作类型（如$set）
// 参数 k: 字段名
func (u Update) Remove(opt string, k string) {
	if vs, ok := u[opt]; ok {
		delete(vs, k)
	}
}

// Set 设置字段值（$set操作）
// 参数 k: 字段名
// 参数 v: 字段值
func (u Update) Set(k string, v interface{}) {
	u.Any(UpdateTypeSet, k, v)
}

// SetOnInsert 仅在插入时设置字段值（$setOnInsert操作）
// 参数 k: 字段名
// 参数 v: 字段值
func (u Update) SetOnInsert(k string, v interface{}) {
	u.Any(UpdateTypeSetOnInsert, k, v)
}

// Inc 增加/减少字段值（$inc操作）
// 参数 k: 字段名
// 参数 v: 增加/减少的值（正数增加，负数减少）
func (u Update) Inc(k string, v interface{}) {
	u.Any(UpdateTypeInc, k, v)
}

// Min 设置字段的最小值（$min操作）
// 仅当新值小于当前值时才更新
// 参数 k: 字段名
// 参数 v: 比较值
func (u Update) Min(k string, v interface{}) {
	u.Any("$min", k, v)
}

// Max 设置字段的最大值（$max操作）
// 仅当新值大于当前值时才更新
// 参数 k: 字段名
// 参数 v: 比较值
func (u Update) Max(k string, v interface{}) {
	u.Any("$max", k, v)
}

// Unset 删除字段（$unset操作）
// 参数 k: 字段名
func (u Update) Unset(k string) {
	u.Any(UpdateTypeUnset, k, 1)
}

// Pop 从数组中删除元素（$pop操作）
// 参数 k: 字段名
// 参数 v: 1表示删除最后一个元素，-1表示删除第一个元素
func (u Update) Pop(k string, v interface{}) {
	u.Any("$pop", k, v)
}

// Pull 从数组中删除匹配的元素（$pull操作）
// 参数 k: 字段名
// 参数 v: 匹配条件
func (u Update) Pull(k string, v interface{}) {
	u.Any("$pull", k, v)
}

// Push 向数组中添加元素（$push操作）
// 参数 k: 字段名
// 参数 v: 要添加的元素
func (u Update) Push(k string, v interface{}) {
	u.Any("$push", k, v)
}

// Any 执行任意类型的更新操作
// 参数 t: 更新操作类型（如$set, $inc）
// 参数 k: 字段名
// 参数 v: 字段值
func (u Update) Any(t, k string, v interface{}) {
	if !strings.HasPrefix(t, "$") {
		t = "$" + t
	}
	if _, ok := u[t]; !ok {
		u[t] = bson.M{}
	}
	u[t][k] = v
}

// Save 批量设置字段值
// 参数 vs: 包含字段名和值的map
func (u Update) Save(vs map[string]any) {
	if _, ok := u[UpdateTypeSet]; !ok {
		u[UpdateTypeSet] = vs
	} else {
		for k, v := range vs {
			u[UpdateTypeSet][k] = v
		}
	}
}

// Convert 将结构体转换为指定类型的更新操作
// 参数 t: 更新操作类型
// 参数 i: 结构体实例
// 返回值: 转换过程中的错误
func (u Update) Convert(t string, i interface{}) error {
	values, err := utils.ToBson(i)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(t, "$") {
		t = "$" + t
	}
	if _, ok := u[t]; !ok {
		u[t] = bson.M{}
	}
	for k, v := range values {
		u[t][k] = v
	}
	return nil
}

// String 将Update转换为JSON字符串
// 返回值: JSON字符串表示
func (u Update) String() string {
	b, _ := json.Marshal(u)
	return string(b)
}

// Projection 生成投影
// 返回值: 包含更新字段的投影（仅包含$set和$inc中的字段）
func (u Update) Projection() bson.M {
	p := make(bson.M)
	for _, m := range projectionField {
		for k, _ := range u[m] {
			p[k] = 1
		}
	}
	return p
}

// Transform 将结构体字段名转换为数据库字段名
// 参数 sch: 模型schema
// 返回值: 转换后的Update实例
func (u Update) Transform(sch *schema.Schema) Update {
	r := Update{}
	for _, t := range []string{UpdateTypeSet, UpdateTypeInc, UpdateTypeUnset, UpdateTypeSetOnInsert} {
		if m, ok := u[t]; ok {
			d := bson.M{}
			for k, v := range m {
				// 如果字段名包含点号，直接使用
				if strings.Contains(k, MongodbFieldSplit) {
					d[k] = v
					// 否则使用schema转换字段名
				} else if field := sch.LookUpField(k); field != nil {
					db := field.DBName()
					d[db] = v
				}
			}
			r[t] = d
		}
	}
	return r
}
