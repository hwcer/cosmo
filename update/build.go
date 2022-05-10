package update

import (
	"errors"
	"fmt"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/schema"
	"github.com/hwcer/cosmo/utils"
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
	"strings"
)

//Build 使用当前模型，将map bson.m Struct 转换成Update
// 如果设置了model,model除主键和零值外其他键值一律作为SetOnInsert值
// 如果设置了model i为bson.m可以使用数据库名和model名
func Build(i interface{}, sch *schema.Schema) (update Update, err error) {
	reflectValue := reflect.Indirect(utils.ValueOf(i))
	switch reflectValue.Kind() {
	case reflect.Map:
		update, err = parseMap(i, sch)
	case reflect.Struct:
		update, err = parseStruct(reflectValue, sch)
	default:
		err = fmt.Errorf("类型错误:%v", reflectValue.Kind())
	}

	//if err != nil || model == nil {
	//	return
	//}
	////setOnInsert
	//reflectModel := reflect.Indirect(utils.ValueOf(model))
	//if !reflectModel.IsValid() || reflectModel.IsZero() {
	//	return
	//}
	//for _, field := range schemaModel.Fields {
	//	if field.DBName == clause.MongoPrimaryName || update.Has(field.DBName) {
	//		continue
	//	}
	//	v := reflectModel.FieldByIndex(field.StructField.Index)
	//	if v.IsValid() && !v.IsZero() {
	//		update.SetOnInert(field.DBName, v.Interface())
	//	}
	//}
	return
}

func parseMap(dest interface{}, sch *schema.Schema) (update Update, err error) {
	var destMap bson.M
	switch dest.(type) {
	case map[string]interface{}:
		destMap = bson.M(dest.(map[string]interface{}))
	case bson.M:
		destMap = dest.(bson.M)
	case Update:
		return dest.(Update), nil //TODO
	default:
		err = errors.New("Update方法参数仅支持 struct 和 map[string]interface{}")
	}
	if err != nil {
		return
	}
	update = make(Update)
	for k, v := range destMap {
		if strings.HasPrefix(k, clause.QueryOperationPrefix) {
			if err = update.Convert(k, v); err != nil {
				return
			}
		} else {
			if sch != nil {
				update.Set(sch.FieldDBName(k), v)
			} else {
				update.Set(k, v)
			}
		}
	}
	return
}

func parseStruct(reflectValue reflect.Value, sch *schema.Schema) (update Update, err error) {
	update = make(Update)
	for _, field := range sch.Fields {
		v := reflectValue.FieldByIndex(field.StructField.Index)
		if v.IsValid() && !v.IsZero() {
			if field.DBName != clause.MongoPrimaryName {
				update.Set(field.DBName, v.Interface())
			}
		}
	}
	return
}
