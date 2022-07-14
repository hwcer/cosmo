package update

import (
	"errors"
	"fmt"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/statement"
	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
)

type SetOnInsert interface {
	SetOnInsert() (map[string]interface{}, error)
}

//Build 使用当前模型，将map bson.m Struct 转换成Update
// 如果设置了model i为bson.m可以使用数据库名和model名
// selects 针对Struct更新时选择，或者忽略的字段，如果为空，更新所有非零值字段
func Build(i interface{}, stmt statement.Statement) (update Update, err error) {
	reflectValue := reflect.Indirect(utils.ValueOf(i))
	switch reflectValue.Kind() {
	case reflect.Map:
		update, err = parseMap(i, stmt)
	case reflect.Struct:
		update, err = parseStruct(reflectValue, stmt)
	default:
		err = fmt.Errorf("类型错误:%v", reflectValue.Kind())
	}
	if err != nil {
		return
	}
	if s, ok := i.(SetOnInsert); ok {
		var v map[string]interface{}
		if v, err = s.SetOnInsert(); err == nil && v != nil {
			update[UpdateTypeSetOnInsert] = v
		}
	}
	return
}

//parseMap 使用Map修改数据 map,bson.M 被视为使用 $set 操作
//高级提交需要使用 Update
func parseMap(dest interface{}, stmt statement.Statement) (update Update, err error) {
	var destMap bson.M
	switch dest.(type) {
	case map[string]interface{}:
		destMap = bson.M(dest.(map[string]interface{}))
	case bson.M:
		destMap = dest.(bson.M)
	case Update:
		return dest.(Update), nil
	default:
		err = errors.New("Update方法参数仅支持 struct 和 map[string]interface{}")
	}
	if err != nil {
		return
	}
	sch := stmt.Schema()
	update = make(Update)
	if sch != nil {
		update[UpdateTypeSet] = destMap
	} else {
		for k, v := range destMap {
			if field := sch.LookUpField(k); field != nil {
				update.Set(field.DBName, v)
			}
		}
	}
	return
}

func parseStruct(reflectValue reflect.Value, stmt statement.Statement) (update Update, err error) {
	update = make(Update)
	sch := stmt.Schema()
	projection := stmt.Projection()
	selects := int(-1)
	if len(projection) > 0 {
		for _, selects = range projection {
			break
		}
	}
	for _, field := range sch.Fields {
		v := reflectValue.FieldByIndex(field.StructField.Index)
		if !v.IsValid() || field.DBName == clause.MongoPrimaryName {
			continue
		}
		if selects >= 0 {
			if _, ok := projection[field.DBName]; (selects == 0 && !ok) || (selects == 1 && ok) {
				update.Set(field.DBName, v.Interface())
			}
		} else if !v.IsZero() {
			update.Set(field.DBName, v.Interface())
		}
	}

	return
}
