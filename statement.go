package cosmo

import (
	"context"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/schema"
	"go.mongodb.org/mongo-driver/bson"
	"reflect"
)

func NewStatement(db *DB) *Statement {
	return &Statement{
		DB:       db,
		Context:  context.Background(),
		Clause:   clause.New(),
		paging:   &Paging{},
		settings: map[string]interface{}{},
	}
}

// Statement statement
type Statement struct {
	*DB
	Dest         interface{}
	Table        string
	Model        interface{}
	ReflectValue reflect.Value
	//ReflectModel reflect.Value
	Omits    []string // omit columns
	Selects  []string // selected columns
	Context  context.Context
	Clause   *clause.Query
	paging   *Paging
	schema   *schema.Schema
	settings map[string]interface{}
	multiple bool
}

//Parse Parse model to schema
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
			_ = tx.Errorf(ErrInvalidValue)
			return
		}
	}

	var err error
	if stmt.Model != nil {
		stmt.schema, err = schema.Parse(stmt.Model, Options)
	} else {
		stmt.schema, err = schema.Parse(stmt.ReflectValue, Options)
	}
	if err != nil {
		tx.Errorf(err)
		return
	}
	if stmt.Table == "" {
		stmt.Table = stmt.schema.Table
	}
	//空查询，匹配Dest或者Model中的主键
	if stmt.Clause.Len() == 0 {
		var reflectValue reflect.Value
		if stmt.Model != nil {
			reflectValue = reflect.Indirect(reflect.ValueOf(stmt.Model))
		} else if stmt.ReflectValue.Kind() == reflect.Struct {
			reflectValue = stmt.ReflectValue
		}
		if reflectValue.IsValid() && !reflectValue.IsZero() {
			if v := tx.Statement.schema.GetValue(reflectValue, clause.MongoPrimaryName); v != nil {
				tx.Where(v)
			}
		}
	}
	return
}

//DBName 将对象字段转换成数据库字段
func (stmt *Statement) DBName(name string) string {
	if stmt.schema == nil {
		return name
	}
	if field := stmt.schema.LookUpField(name); field != nil {
		return field.DBName
	}
	return name
}

//Order 排序
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

//Projection 不能同时使用Select和Omit 优先Select生效
//可以使用model属性名或者数据库字段名
func (stmt *Statement) Projection() map[string]int {
	projection := make(map[string]int)
	for _, k := range stmt.Selects {
		projection[stmt.DBName(k)] = 1
	}
	if len(projection) > 0 {
		return projection
	}
	for _, k := range stmt.Omits {
		projection[stmt.DBName(k)] = 0
	}
	return projection
}
