package update

import (
	"encoding/json"
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

const (
	UpdateTypeSet         = "$set"
	UpdateTypeInc         = "$inc"
	UpdateTypeUnset       = "$unset"
	UpdateTypeSetOnInsert = "$setOnInsert"
)

var projectionField = []string{UpdateTypeSet, UpdateTypeInc}

func New() Update {
	return make(Update)
}

func NewFromMap(v map[string]any) Update {
	r := make(Update)
	r.MSet(v)
	return r
}

type Update map[string]bson.M

func (u Update) Has(opt string, filed string) bool {
	if vs, ok := u[opt]; !ok {
		return false
	} else {
		_, ok = vs[filed]
		return ok
	}
}
func (u Update) Get(opt string, k string) (v any, ok bool) {
	var vs bson.M
	if vs, ok = u[opt]; ok {
		v, ok = vs[k]
	}
	return
}

func (u Update) Remove(opt string, k string) {
	if vs, ok := u[opt]; ok {
		delete(vs, k)
	}
}

func (u Update) Set(k string, v interface{}) {
	u.Any(UpdateTypeSet, k, v)
}

func (u Update) SetOnInert(k string, v interface{}) {
	u.Any(UpdateTypeSetOnInsert, k, v)
}

func (u Update) Inc(k string, v interface{}) {
	u.Any(UpdateTypeInc, k, v)
}

func (u Update) Min(k string, v interface{}) {
	u.Any("$min", k, v)
}

func (u Update) Max(k string, v interface{}) {
	u.Any("$max", k, v)
}

func (u Update) Unset(k string) {
	u.Any(UpdateTypeUnset, k, 1)
}

func (u Update) Pop(k string, v interface{}) {
	u.Any("$pop", k, v)
}

func (u Update) Pull(k string, v interface{}) {
	u.Any("$pull", k, v)
}

func (u Update) Push(k string, v interface{}) {
	u.Any("$push", k, v)
}

func (u Update) Any(t, k string, v interface{}) {
	if !strings.HasPrefix(t, "$") {
		t = "$" + t
	}
	if _, ok := u[t]; !ok {
		u[t] = bson.M{}
	}
	u[t][k] = v
}
func (u Update) MSet(vs map[string]any) {
	if _, ok := u[UpdateTypeSet]; !ok {
		u[UpdateTypeSet] = vs
	} else {
		for k, v := range vs {
			u[UpdateTypeSet][k] = v
		}
	}
}

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

func (u Update) String() string {
	b, _ := json.Marshal(u)
	return string(b)
}

func (u Update) Projection() bson.M {
	p := make(bson.M)
	for _, m := range projectionField {
		for k, _ := range u[m] {
			p[k] = 1
		}
	}
	return p
}

// Transform 转换成数据库字段名
func (u Update) Transform(sch *schema.Schema) Update {
	r := Update{}
	for _, t := range []string{UpdateTypeSet, UpdateTypeInc, UpdateTypeUnset, UpdateTypeSetOnInsert} {
		if m, ok := u[t]; ok {
			d := bson.M{}
			for k, v := range m {
				if strings.Contains(k, MongodbFieldSplit) {
					d[k] = v
				} else if field := sch.LookUpField(k); field != nil {
					d[field.DBName] = v
				}
			}
			r[t] = d
		}
	}
	return r
}
