package update

import (
	"fmt"
	"reflect"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/utils"
	"github.com/hwcer/logger"
)

// MongodbFieldSplit MongoDB字段分隔符
const MongodbFieldSplit = "."

// SetOnInsert 定义仅在插入时设置字段的接口
// 实现此接口的结构体可以在插入时设置特定字段

type SetOnInsert interface {
	// SetOnInsert 返回仅在插入时设置的字段映射
	// 返回值: 字段映射和可能的错误
	SetOnInsert() (map[string]any, error)
}

// iStmt 定义语句接口
// 用于从语句中获取构建Update所需的信息

type iStmt interface {
	GetValue() any               // 获取值
	GetSchema() *schema.Schema   // 获取模型schema
	GetSelector() *Selector      // 获取字段选择器
	GetReflectValue() reflect.Value // 获取反射值
	GetIncludeZeroValue() bool   // 获取是否包含零值
}

// Build 将各种类型（map、bson.M、Struct）转换为Update
// 参数 i: 要转换的值，可以是map、bson.M、Struct或Update类型
// 参数 sch: 模型schema，用于字段名映射
// 参数 filter: 字段选择器，用于指定要更新的字段
// 参数 includeZeroValue: 是否包含零值字段
// 返回值: Update实例、是否需要upsert、可能的错误
func Build(i any, sch *schema.Schema, filter *Selector, includeZeroValue bool) (update Update, upsert bool, err error) {
	reflectValue := reflect.Indirect(utils.ValueOf(i))
	switch reflectValue.Kind() {
	case reflect.Map:
		update, err = parseMap(i, sch)
	case reflect.Struct:
		update, err = parseStruct(i, reflectValue, sch, filter, includeZeroValue)
	default:
		err = fmt.Errorf("类型错误:%v", reflectValue.Kind())
	}
	if err != nil {
		return
	}

	// 处理SetOnInsert操作
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

// BuildWithStmt 使用语句构建Update
// 参数 stmt: 语句接口，提供构建Update所需的信息
// 返回值: Update实例、是否需要upsert、可能的错误
func BuildWithStmt(stmt iStmt) (update Update, upsert bool, err error) {
	return Build(stmt.GetValue(), stmt.GetSchema(), stmt.GetSelector(), stmt.GetIncludeZeroValue())
}

// parseMap 解析Map类型的值为Update
// map和bson.M类型默认使用$set操作
// 高级更新操作需要直接使用Update类型
// 参数 desc: Map类型的值
// 参数 sch: 模型schema，用于字段名映射
// 返回值: Update实例和可能的错误
func parseMap(desc interface{}, sch *schema.Schema) (update Update, err error) {
	switch v := desc.(type) {
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
	if sch != nil {
		return update.Transform(sch), nil
	} else {
		return update, nil
	}
}

// parseStruct 解析Struct类型的值为Update
// 参数 desc: Struct类型的值
// 参数 reflectValue: Struct的反射值
// 参数 sch: 模型schema，用于字段名映射
// 参数 filter: 字段选择器，用于指定要更新的字段
// 参数 includeZeroValue: 是否包含零值字段
// 返回值: Update实例和可能的错误
func parseStruct(desc interface{}, reflectValue reflect.Value, sch *schema.Schema, filter *Selector, includeZeroValue bool) (update Update, err error) {
	defer func() {
		if e := recover(); e != nil {
			logger.Error("%v", e)
		}
	}()
	
	// 如果没有提供schema，自动解析
	if sch == nil {
		if sch, err = schema.Parse(desc); err != nil {
			return
		}
	}
	
	update = make(Update)
	
	// 遍历模型字段
	sch.Range(func(field *schema.Field) bool {
		k := field.DBName()
		// 跳过主键字段
		if k == clause.MongoPrimaryName {
			return true
		}
		
		v := reflectValue.FieldByIndex(field.Index)
		// 如果字段在选择器中且有效
		if filter.Has(k) && v.IsValid() {
			// 如果包含零值或者字段不是零值
			if includeZeroValue || !v.IsZero() {
				update.Set(k, v.Interface())
			}
		}
		return true
	})
	
	// 如果结构体实现了SetOnInsert接口，处理插入时的字段设置
	if s, ok := desc.(SetOnInsert); ok {
		var v map[string]interface{}
		if v, err = s.SetOnInsert(); err == nil && len(v) > 0 {
			update[UpdateTypeSetOnInsert] = v
		}
	}
	
	return
}

// filterSetOnInsert 过滤SetOnInsert操作中的字段
// 移除已经在其他更新操作中设置的字段
// 参数 data: SetOnInsert的字段映射
// 参数 update: Update实例，包含其他更新操作
// 返回值: 过滤后的SetOnInsert字段映射
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
