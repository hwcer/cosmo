package schema

import (
	"errors"
	"fmt"
	"reflect"
)

// ErrUnsupportedDataType unsupported data type
var ErrUnsupportedDataType = errors.New("unsupported data type")

type Schema struct {
	err            error
	initialized    chan struct{}
	Name           string
	Table          string
	Fields         []*Field
	ModelType      reflect.Type
	FieldsByName   map[string]*Field
	FieldsByDBName map[string]*Field
}

func (schema Schema) String() string {
	if schema.ModelType.Name() == "" {
		return fmt.Sprintf("%s(%s)", schema.Name, schema.Table)
	}
	return fmt.Sprintf("%s.%s", schema.ModelType.PkgPath(), schema.ModelType.Name())
}

func (schema Schema) New() reflect.Value {
	results := reflect.New(reflect.PtrTo(schema.ModelType))
	return results
}

func (schema Schema) MakeSlice() reflect.Value {
	slice := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(schema.ModelType)), 0, 20)
	results := reflect.New(slice.Type())
	results.Elem().Set(slice)
	return results
}

func (schema Schema) LookUpField(name string) *Field {
	if field, ok := schema.FieldsByDBName[name]; ok {
		return field
	}
	if field, ok := schema.FieldsByName[name]; ok {
		return field
	}
	return nil
}

// FieldDBName 查询对象字段对应的DBName
func (schema Schema) FieldDBName(name string) string {
	if field := schema.LookUpField(name); field != nil {
		return field.DBName
	}
	return name
}
