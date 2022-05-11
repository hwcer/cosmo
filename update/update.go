package update

import (
	"encoding/json"
	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

const (
	UpdateTypeSet         = "$set"
	UpdateTypeInc         = "$inc"
	UpdateTypeSetOnInsert = "$setOnInsert"
)

func New() Update {
	return make(Update)
}

type Update map[string]bson.M

func (u Update) Has(filed string) bool {
	for _, v := range u {
		if _, has := v[filed]; has {
			return true
		}
	}
	return false
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

func (u Update) UnSet(k string, v interface{}) {
	u.Any("$unset", k, v)
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

var projectionField = []string{UpdateTypeSet, UpdateTypeInc}

func (u Update) Projection() bson.M {
	p := make(bson.M)
	for _, m := range projectionField {
		for k, _ := range u[m] {
			p[k] = 1
		}
	}
	return p
}
