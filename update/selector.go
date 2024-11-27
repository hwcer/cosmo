package update

import (
	"github.com/hwcer/cosgo/schema"
)

type SelectorType int8

const (
	SelectorTypeNone SelectorType = iota
	SelectorTypeOmit
	SelectorTypeSelect
)

type Selector struct {
	selector   SelectorType
	projection map[string]bool
}

// Has 是否被选择
func (this *Selector) Has(key string) bool {
	_, ok := this.projection[key]
	if this.selector == SelectorTypeOmit {
		return !ok
	} else {
		return ok
	}
}

func (this *Selector) Release() {
	this.selector = SelectorTypeNone
	this.projection = nil
}

// Select specify fields that you want when querying, creating, updating
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

// Omit specify fields that you want to ignore when creating, updating and querying
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

// Projection 获取字段,如果sch!=nil && this.selector == SelectorTypeOmit 全部翻转成 Select模式
// FindOneAndUpdate 时有用,其他模式传nil
func (this *Selector) Projection(sch *schema.Schema) map[string]bool {
	if this.projection == nil {
		return nil
	}
	r := map[string]bool{}
	for k, v := range this.projection {
		if field := sch.LookUpField(k); field != nil {
			r[field.DBName] = v
		}
	}

	return r
}
