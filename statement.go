package cosmo

import (
	"context"
	"reflect"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/bson"
)

// NewStatement 创建一个新的Statement实例
// 参数 db: 数据库连接实例
// 返回值: 初始化的Statement实例
func NewStatement(db *DB) *Statement {
	return &Statement{
		DB:      db,
		Context: context.Background(),
		Clause:  clause.New(),
		Paging:  &Paging{},
	}
}

// Statement 表示一个MongoDB数据库操作语句
// 用于构建和执行查询、插入、更新、删除等操作
// 包含操作所需的所有信息，如查询条件、排序、分页、更新字段等
type Statement struct {
	*DB                                  // 数据库连接实例
	model                any             // 操作的模型对象，用于ORM映射
	value                any             // 存储查询结果或写入数据的对象
	table                string          // 操作的集合名称
	Clause               *clause.Query   // 查询条件构建器
	Paging               *Paging         // 分页信息
	Context              context.Context // 操作上下文
	schema               *schema.Schema  // 模型的元数据信息
	orders               map[string]int  // 排序字段映射(1:升序, -1:降序)
	upsert               bool            // 文档不存在时是否自动插入(upsert操作)
	selector             update.Selector // 更新时的字段选择器
	multiple             bool            // 是否强制批量更新
	reflectValue         reflect.Value   // 反射值，用于处理模型对象
	includeZeroValue     bool            // 更新时是否包含零值字段
	updateAndModifyModel bool            // 更新成功时是否将结果写入model
	pageUpdateField      string          //分页增量更新的字段名，默认update
}

// Parse 解析模型并初始化Statement
// 处理模型的反射信息、schema映射和表名
// 返回值: 数据库实例，包含可能的错误信息
func (stmt *Statement) Parse() (tx *DB) {
	tx = stmt.DB
	if tx.Error != nil {
		return
	}
	// assign value values
	if stmt.value != nil {
		stmt.reflectValue = reflect.ValueOf(stmt.value)
		for stmt.reflectValue.Kind() == reflect.Ptr {
			if stmt.reflectValue.IsNil() && stmt.reflectValue.CanAddr() {
				stmt.reflectValue.Set(reflect.New(stmt.reflectValue.Type().Elem()))
			}
			stmt.reflectValue = stmt.reflectValue.Elem()
		}
		if !stmt.reflectValue.IsValid() {
			return tx.Errorf(ErrInvalidValue)
		}
	}
	// schema
	if stmt.schema == nil {
		if stmt.model != nil {
			stmt.schema, tx.Error = schema.Parse(stmt.model)
		} else if stmt.table == "" {
			// table 为空时尝试通过value解析
			stmt.schema, _ = schema.Parse(stmt.value)
		}
		if tx.Error != nil {
			return
		}
	}

	if stmt.table == "" {
		if stmt.schema != nil {
			stmt.table = stmt.schema.Table
		} else {
			return tx.Errorf("database table name is nil")
		}
	}
	return
}

// DBName 将结构体字段名转换为数据库字段名
// 参数 name: 结构体字段名
// 返回值: 对应的数据库字段名
func (stmt *Statement) DBName(name string) string {
	if stmt.schema == nil {
		return name
	}
	if field := stmt.schema.LookUpField(name); field != nil {
		return field.DBName()
	}
	return name
}

// Order 生成MongoDB排序条件
// 将内部的排序映射转换为MongoDB的bson.D格式
// 返回值: 排序条件
func (stmt *Statement) Order() (order bson.D) {
	all := map[string]struct{}{}
	for k, v := range stmt.orders {
		k = stmt.DBName(k)
		if _, ok := all[k]; !ok {
			all[k] = struct{}{}
			order = append(order, bson.E{Key: k, Value: v})
		}
	}
	return
}

// GetValue 获取结果存储对象
// 返回值: 用于存储查询结果或写入数据的对象
func (stmt *Statement) GetValue() any {
	return stmt.value
}

// GetSchema 获取模型的schema信息
// 返回值: 模型的元数据信息
func (stmt *Statement) GetSchema() *schema.Schema {
	return stmt.schema
}

// GetSelector 获取更新字段选择器
// 返回值: 更新时的字段选择器
func (stmt *Statement) GetSelector() *update.Selector {
	return &stmt.selector
}

// GetReflectValue 获取模型的反射值
// 返回值: 模型对象的反射值
func (stmt *Statement) GetReflectValue() reflect.Value {
	return stmt.reflectValue
}

// GetIncludeZeroValue 获取是否包含零值的设置
// 返回值: 更新时是否包含零值字段
func (stmt *Statement) GetIncludeZeroValue() bool {
	return stmt.includeZeroValue
}
