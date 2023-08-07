package update

import (
	"errors"
	"fmt"
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/utils"
	"reflect"
)

const MongodbFieldSplit = "."

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
		update, err = parseMap(i, reflectValue, sch, filter)
	case reflect.Struct:
		update, err = parseStruct(i, reflectValue, sch, filter)
	default:
		err = fmt.Errorf("类型错误:%v", reflectValue.Kind())
	}
	if err != nil {
		return
	}

	if v, ok := update[UpdateTypeSetOnInsert]; ok {
		if r := filterSetOnInsert(v, update); len(r) > 0 {
			update[UpdateTypeSetOnInsert] = r
		} else {
			delete(update, UpdateTypeSetOnInsert)
		}
	}

	return
}

// parseMap 使用Map修改数据 map,bson.M 被视为使用 $set 操作
// 高级提交需要使用 Update
func parseMap(desc interface{}, reflectValue reflect.Value, sch *schema.Schema, filter *Selector) (update Update, err error) {
	switch v := desc.(type) {
	case Update:
		update = desc.(Update)
	default:
		update = Update{}
		err = update.Convert(UpdateTypeSet, v)
	}
	if err != nil {
		return
	}
	return update.Transform(sch), nil
}

func parseStruct(desc interface{}, reflectValue reflect.Value, sch *schema.Schema, filter *Selector) (update Update, err error) {
	update = make(Update)
	for _, field := range sch.Fields {
		v := reflectValue.FieldByIndex(field.StructField.Index)
		if v.IsValid() && field.DBName != clause.MongoPrimaryName {
			if has := filter.Has(field.DBName); has > 0 || (has == 0 && !v.IsZero()) {
				update.Set(field.DBName, v.Interface())
			}
		}
	}
	if s, ok := desc.(SetOnInsert); ok {
		var v map[string]interface{}
		if v, err = s.SetOnInsert(); err == nil && len(v) > 0 {
			update[UpdateTypeSetOnInsert] = v
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
