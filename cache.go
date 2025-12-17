package cosmo

import (
	"github.com/hwcer/logger"
	"sort"
	"sync"
	"time"
)

// CacheEventType 缓存事件类型
type CacheEventType int8

const (
	CacheEventTypeCreate CacheEventType = 0 // 创建事件
	CacheEventTypeUpdate CacheEventType = 1 // 更新事件
	CacheEventTypeDelete CacheEventType = 2 // 删除事件
)

// CacheModel 缓存模型接口，用于支持缓存功能的模型需要实现此接口
type CacheModel interface {
	GetUpdate() int64 // 获取模型的更新时间戳
}

// CacheSetter 缓存设置函数类型，用于将模型添加到缓存中
type CacheSetter func(k any, v CacheModel)

// CacheFilter 缓存过滤函数类型，用于过滤缓存数据，返回nil表示过滤失败
type CacheFilter func(v CacheModel) any

// CacheHandle 缓存句柄接口，用于实现缓存的加载和刷新
type CacheHandle interface {
	// Reload 重新加载缓存数据
	// ts: 时间戳，用于加载指定时间之后更新的数据
	// cb: 缓存设置函数，用于将加载的数据添加到缓存中
	Reload(ts int64, cb CacheSetter) error
}

// NewCache 创建一个新的缓存实例
// handle: 缓存句柄，用于加载和刷新缓存数据
func NewCache(handle CacheHandle) *Cache {
	i := &Cache{handle: handle}
	i.time = time.Now().Unix()
	i.dataset = NewCacheData()
	return i
}

// NewCacheData 创建一个新的缓存数据实例
func NewCacheData() *CacheData {
	return &CacheData{dict: make(map[any]CacheModel)}
}

// CacheData 缓存数据结构体，用于存储缓存的模型数据
type CacheData struct {
	dict map[any]CacheModel // 缓存数据映射，键为模型ID，值为模型对象
}

func (this *CacheData) Copy() *CacheData {
	d := NewCacheData()
	for k, v := range this.dict {
		d.dict[k] = v
	}
	return d
}

func (this *CacheData) Delete(id any) *CacheData {
	d := NewCacheData()
	for k, v := range this.dict {
		if k != id {
			d.dict[k] = v
		}
	}
	return d
}

func (this *CacheData) setter(id any, i CacheModel) {
	this.dict[id] = i
}

type Cache struct {
	time    int64
	handle  CacheHandle
	cursor  []CacheModel
	locker  sync.Mutex
	dataset *CacheData
}

func (this *Cache) Len() int {
	return len(this.dataset.dict)
}
func (this *Cache) Get(id string) any {
	return this.dataset.dict[id]
}
func (this *Cache) Has(id string) (ok bool) {
	_, ok = this.dataset.dict[id]
	return
}

func (this *Cache) Lock(f func() error) error {
	this.locker.Lock()
	defer this.locker.Unlock()
	return f()
}
func (this *Cache) Cursor(update int64, filter CacheFilter) []any {
	var cursor []CacheModel
	if len(this.cursor) == 0 {
		this.locker.Lock()
		defer this.locker.Unlock()
		for _, v := range this.dataset.dict {
			cursor = append(cursor, v)
		}
		sort.Slice(cursor, func(i, j int) bool {
			return cursor[i].GetUpdate() > cursor[j].GetUpdate()
		})
		this.cursor = cursor
	} else {
		cursor = this.cursor
	}
	var r []any
	for _, v := range cursor {
		if s := this.filter(v, update, filter); s != nil {
			r = append(r, s)
		}
	}
	return r
}

func (this *Cache) filter(v CacheModel, update int64, filter CacheFilter) any {
	if v.GetUpdate() <= update {
		return nil
	}
	if filter == nil {
		return v
	}
	return filter(v)
}

func (this *Cache) Page(page *Paging, filter CacheFilter) (err error) {
	cursor := this.Cursor(page.Update, filter)
	page.Init(300)
	page.Result(len(cursor))
	if page.Page > page.Total {
		return
	}
	offset := (page.Page - 1) * page.Size
	end := offset + page.Size
	if end > page.Record {
		end = page.Record
	}
	page.Rows = cursor[offset:end]
	return
}

func (this *Cache) Range(f func(any) bool) {
	for _, v := range this.dataset.dict {
		if !f(v) {
			return
		}
	}
}
func (this *Cache) Delete(id string) {
	this.locker.Lock()
	defer this.locker.Unlock()
	this.cursor = nil
	this.dataset = this.dataset.Delete(id)
}

func (this *Cache) Reload(ts int64, handle ...CacheHandle) error {
	if ts > 0 && ts <= this.time {
		return nil
	}
	var h CacheHandle
	if len(handle) > 0 {
		h = handle[0]
	} else {
		h = this.handle
	}

	this.locker.Lock()
	defer this.locker.Unlock()
	dataset := this.dataset.Copy()
	err := h.Reload(ts, dataset.setter)
	if err != nil {
		return err
	}
	if ts > 0 {
		this.time = ts
	}
	this.cursor = nil
	this.dataset = dataset
	return nil
}

// Listener 监听数据库变化
// id 变更数据ID
// update 变化时间
func (this *Cache) Listener(t CacheEventType, id string, update int64) {
	switch t {
	case CacheEventTypeDelete:
		this.Delete(id)
	case CacheEventTypeUpdate, CacheEventTypeCreate:
		if err := this.Reload(update); err != nil {
			logger.Alert("Cache Listener Reload[%v] error[%v]", id, err)
		}
	}
}
