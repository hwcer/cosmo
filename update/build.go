package update

import (
	"fmt"
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/logger"
	"reflect"
)

const MongodbFieldSplit = "."

type SetOnInsert interface {
	SetOnInsert() (map[string]any, error)
}

type iStmt interface {
	GetValue() any
	GetSchema() *schema.Schema
	GetSelector() *Selector
	GetReflectValue() reflect.Value
	GetIncludeZeroValue() bool
}

// Build 使用当前模型，将map bson.m Struct 转换成Update
// 如果设置了model i为bson.m可以使用数据库名和model名
// selects 针对Struct更新时选择，或者忽略的字段，如果为空，更新所有非零值字段
func Build(stmt iStmt) (update Update, upsert bool, err error) {
	reflectValue := reflect.Indirect(stmt.GetReflectValue())
	switch reflectValue.Kind() {
	case reflect.Map:
		update, err = parseMap(stmt)
	case reflect.Struct:
		update, err = parseStruct(stmt)
	default:
		err = fmt.Errorf("类型错误:%v", reflectValue.Kind())
	}
	if err != nil {
		return
	}

	if v, ok := update[UpdateTypeSetOnInsert]; ok {
		if r := filterSetOnInsert(v, update); len(r) > 0 {
			upsert = true
			update[UpdateTypeSetOnInsert] = r
		} else {
			delete(update, UpdateTypeSetOnInsert)
		}
	}

	return
}

// parseMap 使用Map修改数据 map,bson.M 被视为使用 $set 操作
// 高级提交需要使用 Update
func parseMap(stmt iStmt) (update Update, err error) {
	switch v := stmt.GetValue().(type) {
	case Update:
		update = v
	case *Update:
		update = *v
	case map[string]any:
		update = NewFromMap(v)
	default:
		update = Update{}
		err = update.Convert(UpdateTypeSet, v)
	}
	if err != nil {
		return
	}
	if sch := stmt.GetSchema(); sch != nil {
		return update.Transform(sch), nil
	} else {
		return update, nil
	}
}

func parseStruct(stmt iStmt) (update Update, err error) {
	defer func() {
		if e := recover(); e != nil {
			logger.Error("%v", e)
		}
	}()
	desc := stmt.GetValue()
	filter := stmt.GetSelector()
	reflectValue := reflect.Indirect(stmt.GetReflectValue())
	includeZeroValue := stmt.GetIncludeZeroValue()
	sch := stmt.GetSchema()
	if sch == nil {
		if sch, err = schema.Parse(desc); err != nil {
			return
		}
	}
	update = make(Update)
	sch.Range(func(field *schema.Field) bool {
		k := field.DBName()
		if k == clause.MongoPrimaryName {
			return true
		}
		v := reflectValue.FieldByIndex(field.Index)
		if filter.Has(k) && v.IsValid() {
			if includeZeroValue || !v.IsZero() {
				update.Set(k, v.Interface())
			}
		}
		return true
	})
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
