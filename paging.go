package cosmo

import (
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Paging 分页
type Paging struct {
	//order  []bson.E    //排序
	Rows   interface{} `json:"rows"`
	Page   int         `json:"page"`             //当前页
	Size   int         `json:"size"`             //每页大小
	Total  int         `json:"total"`            //总页码数
	Record int         `json:"record"`           //总记录数
	Update int64       `json:"update,omitempty"` //最后更新时间
}

func (this *Paging) Init(size int) {
	if this.Page <= 0 {
		this.Page = 1
	}
	if this.Size <= 0 || this.Size > size {
		this.Size = size
	}
}

func (this *Paging) Result(r int) {
	if this.Size == 0 {
		this.Init(100)
	}
	this.Record = r
	this.Total = r / this.Size
	if r%this.Size != 0 {
		this.Total += 1
	}
}

func (this *Paging) Offset() int {
	return (this.Page - 1) * this.Size
}

// Options 转换成FindOptions
func (this *Paging) Options() *options.FindOptions {
	opts := options.Find()
	opts.SetLimit(int64(this.Size))
	if offset := this.Offset(); offset > 1 {
		opts.SetSkip(int64(offset))
	}
	//if len(this.order) > 0 {
	//	opts.SetSort(this.order)
	//}
	return opts
}

// Range 遍历第N页的索引下标
func (this *Paging) Range(page int, handle func(int)) {
	if page < 1 {
		page = 1
	}
	if page > this.Total {
		return
	}
	s := (page - 1) * this.Size
	if s >= this.Record {
		return
	}
	e := s + this.Size
	if e > this.Record {
		e = this.Record
	}
	for i := s; i < e; i++ {
		handle(i)
	}
}
