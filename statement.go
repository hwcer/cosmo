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
		Paging:   &Paging{},
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
	Schema   *schema.Schema
	Context  context.Context
	Clause   *clause.Query
	Paging   *Paging
	settings map[string]interface{}
	multiple bool
}

//Parse Parse model to Schema
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
			tx.Errorf(ErrInvalidValue)
			return
		}
	}

	var err error
	if stmt.Model != nil {
		stmt.Schema, err = schema.Parse(stmt.Model, Options)
	} else {
		stmt.Schema, err = schema.Parse(stmt.ReflectValue, Options)
	}
	if err != nil {
		tx.Errorf(err)
		return
	}
	if stmt.Table == "" {
		stmt.Table = stmt.Schema.Table
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
			if v := tx.Statement.Schema.GetValue(reflectValue, clause.MongoPrimaryName); v != nil {
				tx.Where(v)
			}
		}
	}
	return
}

//DBName 将对象字段转换成数据库字段
func (stmt *Statement) DBName(name string) string {
	if stmt.Schema == nil {
		return name
	}
	if field := stmt.Schema.LookUpField(name); field != nil {
		return field.DBName
	}
	return name
}

//projection 不能同时使用Select和Omit 优先Select生效
//可以使用model属性名或者数据库字段名
func (stmt *Statement) projection() (projection bson.M, order bson.D, err error) {
	projection = make(bson.M)
	for _, k := range stmt.Selects {
		projection[stmt.DBName(k)] = 1
	}
	for _, k := range stmt.Omits {
		projection[stmt.DBName(k)] = 0
	}
	for _, v := range stmt.Paging.order {
		v.Key = stmt.DBName(v.Key)
		order = append(order, v)
	}
	return
}
