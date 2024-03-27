package cosmo

import (
	"context"
	"reflect"

	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"github.com/hwcer/schema"
	"go.mongodb.org/mongo-driver/bson"
)

func NewStatement(db *DB) *Statement {
	return &Statement{
		DB:      db,
		Context: context.Background(),
		Clause:  clause.New(),
		Paging:  &Paging{},
		//settings: map[string]interface{}{},
	}
}

// Statement statement
type Statement struct {
	*DB
	model                any
	value                any
	table                string
	selector             update.Selector
	reflectValue         reflect.Value
	Context              context.Context
	Clause               *clause.Query
	Paging               *Paging
	schema               *schema.Schema
	upsert               bool //文档不存在时自动插入新文档
	multiple             bool //强制批量更新
	updateAndModifyModel bool //更新数据库成功时修改将最终结果写入到model
}

// Parse Parse model to schema
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

	//var sch *schema.Schema
	if stmt.model != nil {
		stmt.schema, tx.Error = schema.Parse(stmt.model)
	} else {
		stmt.schema, tx.Error = schema.Parse(stmt.reflectValue)
	}
	if tx.Error != nil {
		return
	}
	if stmt.schema == nil {
		return tx.Errorf("schema is nil")
	}
	if stmt.table == "" {
		stmt.table = stmt.schema.Table
	}

	return
}

// DBName 将对象字段转换成数据库字段
func (stmt *Statement) DBName(name string) string {
	if stmt.schema == nil {
		return name
	}
	if field := stmt.schema.LookUpField(name); field != nil {
		return field.DBName
	}
	return name
}

// Order 排序
func (stmt *Statement) Order() (order bson.D) {
	for _, v := range stmt.Paging.order {
		v.Key = stmt.DBName(v.Key)
		order = append(order, v)
	}
	return
}

func (stmt *Statement) Schema() *schema.Schema {
	return stmt.schema
}
