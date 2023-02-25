package update

import (
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/utils"
)

type SelectorType int8

const (
	SelectorTypeNone SelectorType = iota
	SelectorTypeOmit
	SelectorTypeSelect
)

type Selector struct {
	selector   SelectorType
	projection map[string]int
}

// Is 判断key是否被选中
func (this *Selector) Is(key string, isZero bool) bool {
	if len(this.projection) == 0 {
		return !isZero
	}
	_, ok := this.projection[key]
	if this.selector == SelectorTypeSelect {
		return ok
	} else {
		return !ok
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
		this.projection = map[string]int{}
	}
	for _, k := range columns {
		this.projection[k] = 1
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
		this.projection = map[string]int{}
	}
	for _, k := range columns {
		this.projection[k] = 0
	}
	return true
}

// Projection 获取字段,如果sch!=nil && this.selector == SelectorTypeOmit 全部翻转成 Select模式
// FindOneAndUpdate 时有用,其他模式传nil
func (this *Selector) Projection(sch *schema.Schema) map[string]int {
	if !(sch != nil && this.selector == SelectorTypeOmit) {
		return this.projection
	}
	r := map[string]int{}
	var ok bool
	for _, field := range sch.Fields {
		if _, ok = this.projection[field.DBName]; !ok && field.DBName != utils.MongoPrimaryName {
			r[field.DBName] = 1
		}
	}
	return r
}
