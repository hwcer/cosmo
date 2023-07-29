package cosmo

import (
	"fmt"
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"reflect"
)

// Model specify the model you would like to run db operations
//
//	// update all users's name to `hello`
//	db.model(&User{}).Update("name", "hello")
//	// if user's primary key is non-blank, will use it as condition, then will only update the user's name to `hello`
//	db.model(&user).Update("name", "hello")
func (db *DB) Model(value any, modify ...bool) (tx *DB) {
	tx = db.getInstance()
	tx.statement.model = value
	if len(modify) > 0 && modify[0] {
		tx.statement.updateAndModifyModel = true
	}
	return
}

// table specify the table you would like to run db operations
// 使用TABLE 时select,order,Omit 中必须使用数据库字段名称

func (db *DB) Table(name string) (tx *DB) {
	tx = db.getInstance()
	tx.statement.table = name
	return
}

// Upsert update时如果不存在自动insert
func (db *DB) Upsert() (tx *DB) {
	tx = db.getInstance()
	tx.statement.upsert = true
	return
}

// Multiple 强制批量更新
func (db *DB) Multiple() (tx *DB) {
	tx = db.getInstance()
	tx.statement.multiple = true
	return
}

// Omit specify fields that you want to ignore when creating, updating and querying
func (db *DB) Omit(columns ...string) (tx *DB) {
	tx = db.getInstance()
	if !tx.statement.selector.Omit(columns...) {
		tx.Error = ErrOmitOnSelectsExist
	}
	return
}

// Select specify fields that you want when querying, creating, updating
func (db *DB) Select(columns ...string) (tx *DB) {
	tx = db.getInstance()
	if !tx.statement.selector.Select(columns...) {
		tx.Error = ErrSelectOnOmitsExist
	}
	return
}

// FindAndUpdate 查询并更新,需要配合Select使用
//func (db *DB) FindAndUpdate() (tx *DB) {
//	tx = db.getInstance()
//	tx.statement.findAndUpdate = true
//	return
//}

// Where 查询条件
// 参考 query包
func (db *DB) Where(query interface{}, args ...interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.statement.Clause.Where(query, args...)
	return
}

// Page 分页设置 page-当前页，size-每页大小
//func (db *DB) Page(page, size int) (tx *DB) {
//	tx = db.getInstance()
//	tx.statement.paging.Page(page, size)
//	return
//}

// Order specify order when retrieve records from dbname
func (db *DB) Order(key string, value int) (tx *DB) {
	tx = db.getInstance()
	tx.statement.paging.Order(key, value)
	return
}

// Limit specify the number of records to be retrieved
func (db *DB) Limit(limit int) (tx *DB) {
	tx = db.getInstance()
	tx.statement.paging.Limit(limit)
	return
}

// Offset specify the number of records to skip before starting to return the records
func (db *DB) Offset(offset int) (tx *DB) {
	tx = db.getInstance()
	tx.statement.paging.Offset(offset)
	return
}

// Merge 只更新Model,不会修改数据库
// db.model(m).Merge(i)
// 参数支持 Struct,map[string]interface{}
//func (db *DB) Merge(i interface{}) error {
//	tx := db.statement.Parse()
//	if tx.Error != nil {
//		return tx.Error
//	}
//	values, err := update.Build(i, tx.statement.schema, &tx.statement.selector)
//	if err != nil {
//		return err
//	}
//	if err = tx.SetColumn(values[update.UpdateTypeSet]); err != nil {
//		return err
//	}
//	return nil
//}

// SetColumn set column's value to model
//
//	stmt.SetColumn("Name", "jinzhu") // Hooks Method
func (db *DB) SetColumn(data map[string]interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	if db.statement.model == nil {
		return nil
	}
	sch, err := schema.Parse(db.statement.model)
	if err != nil {
		return err
	}
	reflectValue := reflect.ValueOf(db.statement.model)
	//logger.Debug("reflectModel:%+v", reflectModel.Interface())
	for k, v := range data {
		field := sch.LookUpField(k)
		if field != nil && field.DBName != clause.MongoPrimaryName {
			if err = field.Set(reflectValue, v); err != nil {
				return err
			}
		}
	}
	return nil
}
