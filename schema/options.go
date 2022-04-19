package schema

import (
	"fmt"
	"github.com/hwcer/cosmo/utils"
	"go/ast"
	"reflect"
	"sync"
)

//New 新封装schema Store Namer
func New() (i *Options) {
	return &Options{Store: &sync.Map{}, Namer: &NamingStrategy{}}
}

type Options struct {
	Namer Namer
	Store *sync.Map
}

// Parse get data type from dialector
func (this *Options) Parse(dest interface{}) (*Schema, error) {
	return this.ParseWithSpecialTableName(dest, "")
}

// ParseWithSpecialTableName get data type from dialector with extra schema table
func (this *Options) ParseWithSpecialTableName(dest interface{}, specialTableName string) (*Schema, error) {
	if dest == nil {
		return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
	}
	value := utils.ValueOf(dest)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		value = reflect.New(value.Type().Elem())
	}
	modelType := reflect.Indirect(value).Type()

	if modelType.Kind() == reflect.Interface {
		modelType = reflect.Indirect(reflect.ValueOf(dest)).Elem().Type()
	}

	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		if modelType.PkgPath() == "" {
			return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
		}
		return nil, fmt.Errorf("%w: %s.%s", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
	}

	// Cache the Schema for performance,
	// Use the modelType or modelType + schemaTable (if it present) as cache key.
	var schemaCacheKey interface{}
	if specialTableName != "" {
		schemaCacheKey = fmt.Sprintf("%p-%s", modelType, specialTableName)
	} else {
		schemaCacheKey = modelType
	}

	schema := &Schema{
		initialized: make(chan struct{}),
	}
	// Load exist schmema cache, return if exists
	if v, loaded := this.Store.LoadOrStore(schemaCacheKey, schema); loaded {
		s := v.(*Schema)
		<-s.initialized
		return s, s.err
	} else {
		defer close(schema.initialized)
	}

	modelValue := reflect.New(modelType)
	tableName := this.Namer.TableName(modelType.Name())
	if tabler, ok := modelValue.Interface().(Tabler); ok {
		tableName = tabler.TableName()
	}

	if specialTableName != "" && specialTableName != tableName {
		tableName = specialTableName
	}

	schema.Name = modelType.Name()
	schema.ModelType = modelType
	schema.Table = tableName
	schema.FieldsByName = map[string]*Field{}
	schema.FieldsByDBName = map[string]*Field{}
	for i := 0; i < modelType.NumField(); i++ {
		if fieldStruct := modelType.Field(i); ast.IsExported(fieldStruct.Name) {
			if field := schema.ParseField(fieldStruct); field.EmbeddedSchema != nil {
				schema.Fields = append(schema.Fields, field.EmbeddedSchema.Fields...)
			} else {
				schema.Fields = append(schema.Fields, field)
			}
		}
	}

	for _, field := range schema.Fields {
		if field.DBName == "" {
			field.DBName = this.Namer.ColumnName(schema.Table, field.Name)
		}

		if field.DBName != "" {
			// nonexistence or shortest path or first appear prioritized if has permission
			if v, ok := schema.FieldsByDBName[field.DBName]; !ok || ((field.Creatable || field.Updatable || field.Readable) && len(field.BindNames) < len(v.BindNames)) {
				schema.FieldsByDBName[field.DBName] = field
				schema.FieldsByName[field.Name] = field
			}
		}

		if of, ok := schema.FieldsByName[field.Name]; !ok || of.TagSettings["-"] == "-" {
			schema.FieldsByName[field.Name] = field
		}

		field.setupValuerAndSetter()
	}

	defer func() {
		if schema.err != nil {
			this.Store.Delete(modelType)
		}
	}()
	return schema, schema.err
}
