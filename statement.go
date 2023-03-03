package cosmo

import (
	"context"
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
)

func NewStatement(db *DB) *Statement {
	return &Statement{
		DB:      db,
		Context: context.Background(),
		Clause:  clause.New(),
		paging:  &Paging{},
		//settings: map[string]interface{}{},
	}
}

// Statement statement
type Statement struct {
	*DB
	Dest         interface{}
	Table        string
	Model        interface{}
	Selector     update.Selector
	ReflectValue reflect.Value
	Context      context.Context
	Clause       *clause.Query
	paging       *Paging
	schema       *schema.Schema
	//settings   map[string]interface{}
	upsert   bool //todo
	multiple bool
	//projection map[string]int //findAndUpdate 时需要返回的字段
	//findAndUpdate bool           //更新
}

func (stmt *Statement) HasValidModel() bool {
	if stmt.Model == nil {
		return false
	}
	return !reflect.ValueOf(stmt.Model).IsNil()
}

// Parse Parse model to schema
func (stmt *Statement) Parse() (tx *DB) {
	tx = stmt.DB
	if tx.Error != nil {
		return
	}
	// assign Dest values
	if stmt.Dest != nil {
		stmt.ReflectValue = reflect.ValueOf(stmt.Dest)
		for stmt.ReflectValue.Kind() == reflect.Ptr {
			if stmt.ReflectValue.IsNil() && stmt.ReflectValue.CanAddr() {
				stmt.ReflectValue.Set(reflect.New(stmt.ReflectValue.Type().Elem()))
			}
			stmt.ReflectValue = stmt.ReflectValue.Elem()
		}
		if !stmt.ReflectValue.IsValid() {
			return tx.Errorf(ErrInvalidValue)
		}
	}
	var err error
	//var sch *schema.Schema
	if stmt.Model != nil {
		stmt.schema, err = schema.Parse(stmt.Model)
	} else {
		stmt.schema, err = schema.Parse(stmt.ReflectValue)
	}
	if err != nil {
		return tx.Errorf(err)
	}
	if stmt.schema == nil {
		return tx.Errorf("schema is nil")
	}

	if stmt.Table == "" {
		stmt.Table = stmt.schema.Table
	}

	//if stmt.Clause.Len() == 0 {
	//	_ = tx.Errorf(ErrMissingWhereClause)
	//}
	//空查询，匹配Dest或者Model中的主键
	//if stmt.Clause.Len() == 0 {
	//	var reflectValue reflect.Value
	//	if stmt.Model != nil {
	//		reflectValue = reflect.Indirect(reflect.ValueOf(stmt.Model))
	//	} else if stmt.ReflectValue.Kind() == reflect.Struct {
	//		reflectValue = stmt.ReflectValue
	//	}
	//	if reflectValue.IsValid() && !reflectValue.IsZero() {
	//		if v := tx.Statement.schema.GetValue(reflectValue, clause.MongoPrimaryName); v != nil {
	//			tx.Where(v)
	//		}
	//	}
	//}
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
	for _, v := range stmt.paging.order {
		v.Key = stmt.DBName(v.Key)
		order = append(order, v)
	}
	return
}

func (stmt *Statement) Schema() *schema.Schema {
	return stmt.schema
}

// Projection 不能同时使用Select和Omit 优先Select生效
// 可以使用model属性名或者数据库字段名
//func (stmt *Statement) Projection() map[string]int {
//	projection := make(map[string]int)
//	for _, k := range stmt.Selects {
//		projection[stmt.DBName(k)] = 1
//	}
//	if len(projection) > 0 {
//		return projection
//	}
//	for _, k := range stmt.Omits {
//		projection[stmt.DBName(k)] = 0
//	}
//	return projection
//}
