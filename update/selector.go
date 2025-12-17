// update 包提供MongoDB更新操作的相关功能，包括更新条件构建和字段选择
package update

import (
	"github.com/hwcer/cosgo/schema"
)

// SelectorType 字段选择器类型枚举
type SelectorType int8

const (
	SelectorTypeNone SelectorType = iota   // 无选择（默认）
	SelectorTypeOmit SelectorType = iota   // 排除模式：排除指定字段
	SelectorTypeSelect SelectorType = iota // 选择模式：仅选择指定字段
)

// Selector 字段选择器结构体
// 用于在创建、更新和查询操作中指定要包含或排除的字段
type Selector struct {
	selector   SelectorType      // 选择器类型
	projection map[string]bool   // 字段投影映射，键为字段名，值为是否选择
}

// Has 检查指定字段是否被选择
// 参数 key: 字段名（结构体字段名或数据库字段名）
// 返回值: true表示字段被选择，false表示字段被排除
func (this *Selector) Has(key string) bool {
	if this.projection == nil {
		return true //默认选择所有
	}
	_, ok := this.projection[key]
	if this.selector == SelectorTypeOmit {
		return !ok
	} else {
		return ok
	}
}

// Release 释放选择器资源，重置为初始状态
func (this *Selector) Release() {
	this.selector = SelectorTypeNone
	this.projection = nil
}

// Select 设置要选择的字段（选择模式）
// 参数 columns: 要选择的字段名列表
// 返回值: true表示设置成功，false表示当前选择器类型不兼容（已处于排除模式）
// 注意：选择模式和排除模式不能同时使用
func (this *Selector) Select(columns ...string) bool {
	if this.selector == SelectorTypeOmit {
		return false
	}
	if this.selector == SelectorTypeNone {
		this.selector = SelectorTypeSelect
		this.projection = map[string]bool{}
	}
	for _, k := range columns {
		this.projection[k] = true
	}
	return true
}

// Omit 设置要排除的字段（排除模式）
// 参数 columns: 要排除的字段名列表
// 返回值: true表示设置成功，false表示当前选择器类型不兼容（已处于选择模式）
// 注意：排除模式和选择模式不能同时使用
func (this *Selector) Omit(columns ...string) bool {
	if this.selector == SelectorTypeSelect {
		return false
	}
	if this.selector == SelectorTypeNone {
		this.selector = SelectorTypeOmit
		this.projection = map[string]bool{}
	}
	for _, k := range columns {
		this.projection[k] = false
	}
	return true
}

// Projection 获取字段投影映射，支持模型字段名到数据库字段名的映射
// 参数 sch: 可选的模型schema，用于字段名映射（结构体字段名到数据库字段名）
// 返回值: 字段投影映射，键为数据库字段名，值为是否选择
// 注意：在FindOneAndUpdate等操作中使用时，需要将排除模式转换为选择模式
func (this *Selector) Projection(sch *schema.Schema) map[string]bool {
	if this.projection == nil {
		return nil
	}
	r := map[string]bool{}
	for k, v := range this.projection {
		db := k
		if sch != nil {
			if field := sch.LookUpField(k); field != nil {
				db = field.DBName()
			}
		}
		r[db] = v
	}

	return r
}
