package update

import (
	"errors"
	"fmt"
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
)

type SetOnInsert interface {
	SetOnInsert() (map[string]interface{}, error)
}

// Build 使用当前模型，将map bson.m Struct 转换成Update
// 如果设置了model i为bson.m可以使用数据库名和model名
// selects 针对Struct更新时选择，或者忽略的字段，如果为空，更新所有非零值字段
func Build(i interface{}, sch *schema.Schema, filter *Selector) (update Update, err error) {
	if sch == nil {
		return nil, errors.New("schema is nil")
	}
	reflectValue := reflect.Indirect(utils.ValueOf(i))
	switch reflectValue.Kind() {
	case reflect.Map:
		update, err = parseMap(i, sch, filter)
	case reflect.Struct:
		update, err = parseStruct(i, reflectValue, sch, filter)
	default:
		err = fmt.Errorf("类型错误:%v", reflectValue.Kind())
	}
	if err != nil {
		return
	}

	return
}

// parseMap 使用Map修改数据 map,bson.M 被视为使用 $set 操作
// 高级提交需要使用 Update
func parseMap(dest interface{}, sch *schema.Schema, filter *Selector) (update Update, err error) {
	var destMap bson.M
	switch v := dest.(type) {
	case map[string]interface{}:
		destMap = v
	case bson.M:
		destMap = v
	case Update:
		return dest.(Update), nil
	default:
		err = errors.New("Update方法参数仅支持 struct 和 map[string]interface{}")
	}
	if err != nil {
		return
	}
	update = make(Update)
	for k, v := range destMap {
		if field := sch.LookUpField(k); field != nil {
			name := field.DBName
			if filter.Is(name, false) {
				update.Set(name, v)
			}
		}
	}
	if v, ok := update[UpdateTypeSetOnInsert]; ok {
		update[UpdateTypeSetOnInsert] = filterSetOnInsert(v, update)
	}
	return
}

func parseStruct(dest interface{}, reflectValue reflect.Value, sch *schema.Schema, filter *Selector) (update Update, err error) {
	update = make(Update)
	for _, field := range sch.Fields {
		v := reflectValue.FieldByIndex(field.StructField.Index)
		if v.IsValid() && field.DBName != clause.MongoPrimaryName && filter.Is(field.DBName, v.IsZero()) {
			update.Set(field.DBName, v.Interface())
		}
	}
	if s, ok := dest.(SetOnInsert); ok {
		var v map[string]interface{}
		if v, err = s.SetOnInsert(); err == nil && v != nil {
			update[UpdateTypeSetOnInsert] = filterSetOnInsert(v, update)
		}
	}
	return
}

func filterSetOnInsert(data map[string]interface{}, update Update) map[string]interface{} {
	r := map[string]interface{}{}
	keys := update.Projection()
	for k, v := range data {
		if _, ok := keys[k]; !ok {
			r[k] = v
		}
	}
	return r
}
