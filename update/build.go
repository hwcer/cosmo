package update

import (
	"errors"
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
func Build(i interface{}, store *schema.Store, model interface{}) (update Update, err error) {
	var schemaModel *schema.Schema
	if model != nil {
		if schemaModel, err = store.Parse(model); err != nil {
			return
		}
	}
	reflectValue := reflect.Indirect(utils.ValueOf(i))
	switch reflectValue.Kind() {
	case reflect.Map:
		update, err = parseMap(i, schemaModel)
	case reflect.Struct:
		update, err = parseStruct(reflectValue, store)
	default:
		err = errors.New("类型错误")
	}
	if err != nil || model == nil {
		return
	}
	//setOnInsert
	reflectModel := reflect.Indirect(utils.ValueOf(model))
	if !reflectModel.IsValid() || reflectModel.IsZero(){
		return
	}
	for _, field := range schemaModel.Fields {
		if field.DBName == clause.MongoPrimaryName || update.Has(field.DBName) {
			continue
		}
		v := reflectModel.FieldByIndex(field.StructField.Index)
		if v.IsValid() && !v.IsZero() {
			update.SetOnInert(field.DBName, v.Interface())
		}
	}
	return
}

func parseMap(dest interface{}, sch *schema.Schema) (update Update, err error) {
	var destMap bson.M
	switch dest.(type) {
	case map[string]interface{}:
		destMap = bson.M(dest.(map[string]interface{}))
	case bson.M:
		destMap = dest.(bson.M)
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

func parseStruct(reflectValue reflect.Value, store *schema.Store) (update Update, err error) {
	var destSchema *schema.Schema
	if destSchema, err = store.Parse(reflectValue); err != nil {
		return
	}
	update = make(Update)
	for _, field := range destSchema.Fields {
		v := reflectValue.FieldByIndex(field.StructField.Index)
		if v.IsValid() && !v.IsZero() {
			if field.DBName != clause.MongoPrimaryName {
				update.Set(field.DBName, v.Interface())
			}
		}
	}
	return
}
