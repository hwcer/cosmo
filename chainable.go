package cosmo

import (
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/update"
	"reflect"
)

// Model specify the model you would like to run db operations
//
//	// update all users's name to `hello`
//	db.Model(&User{}).Update("name", "hello")
//	// if user's primary key is non-blank, will use it as condition, then will only update the user's name to `hello`
//	db.Model(&user).Update("name", "hello")
func (db *DB) Model(value interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Model = value
	return
}

// Table specify the table you would like to run db operations
// 使用TABLE 时select,order,Omit 中必须使用数据库字段名称

func (db *DB) Table(name string) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Table = name
	return
}

// Upsert update时如果不存在自动insert
func (db *DB) Upsert() (tx *DB) {
	tx = db.getInstance()
	tx.Statement.upsert = true
	return
}

// Multiple 强制批量操作
func (db *DB) Multiple() (tx *DB) {
	tx = db.getInstance()
	tx.Statement.multiple = true
	return
}

//
//// SetProjection FindAndUpdate时要求返回的字段
//func (db *DB) SetProjection(keys ...string) (tx *DB) {
//	tx = db.getInstance()
//	if tx.Statement.projection == nil {
//		tx.Statement.projection = map[string]int{}
//	}
//	for _, k := range keys {
//		tx.Statement.projection[k] = 1
//	}
//	return
//}
//
//// OmitProjection 除忽略掉的字段外全部返回,必须先设置好MODEL
//func (db *DB) OmitProjection(keys ...string) (tx *DB) {
//	tx = db.getInstance()
//	if tx.Statement.Model == nil {
//		return tx.Errorf(ErrModelValueRequired)
//	}
//	omit := map[string]bool{}
//	for _, k := range keys {
//		omit[k] = true
//	}
//	sch, err := schema.Parse(db.Statement.Model)
//	if err != nil {
//		return tx.Errorf(err)
//	}
//	var ok bool
//	for _, field := range sch.Fields {
//		if _, ok = omit[field.DBName]; !ok {
//			tx = tx.SetProjection(field.DBName)
//		}
//	}
//	return tx
//}

// Omit specify fields that you want to ignore when creating, updating and querying
func (db *DB) Omit(columns ...string) (tx *DB) {
	tx = db.getInstance()
	if !tx.Statement.Selector.Omit(columns...) {
		tx.Error = ErrOmitOnSelectsExist
	}
	return
}

// Select specify fields that you want when querying, creating, updating
func (db *DB) Select(columns ...string) (tx *DB) {
	tx = db.getInstance()
	if !tx.Statement.Selector.Select(columns...) {
		tx.Error = ErrSelectOnOmitsExist
	}
	return
}

// FindAndUpdate 查询并更新,需要配合Select使用
//func (db *DB) FindAndUpdate() (tx *DB) {
//	tx = db.getInstance()
//	tx.Statement.findAndUpdate = true
//	return
//}

// Where 查询条件
// 参考 query包
func (db *DB) Where(query interface{}, args ...interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Clause.Where(query, args...)
	return
}

// Page 分页设置 page-当前页，size-每页大小
func (db *DB) Page(page, size int) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.paging.Page(page, size)
	return
}

// Order specify order when retrieve records from dbname
func (db *DB) Order(key string, value int) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.paging.Order(key, value)
	return
}

// Limit specify the number of records to be retrieved
func (db *DB) Limit(limit int) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.paging.Limit(limit)
	return
}

// Offset specify the number of records to skip before starting to return the records
func (db *DB) Offset(offset int) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.paging.Offset(offset)
	return
}

// Merge 只更新Model,不会修改数据库
// db.Model(m).Merge(i)
// 参数支持 Struct,map[string]interface{}
func (db *DB) Merge(i interface{}) error {
	tx := db.Statement.Parse()
	if tx.Error != nil {
		return tx.Error
	}
	values, err := update.Build(i, tx.Statement.schema, &tx.Statement.Selector)
	if err != nil {
		return err
	}
	if err = tx.SetColumn(values[update.UpdateTypeSet]); err != nil {
		return err
	}
	return nil
}

// SetColumn set column's value to model
//
//	stmt.SetColumn("Name", "jinzhu") // Hooks Method
func (db *DB) SetColumn(data map[string]interface{}) error {
	if db.Statement.Model == nil {
		return nil
	}
	sch, err := schema.Parse(db.Statement.Model)
	if err != nil {
		return err
	}
	reflectValue := reflect.ValueOf(db.Statement.Model)
	//logger.Debug("reflectModel:%+v", reflectModel.Interface())
	for k, v := range data {
		field := sch.LookUpField(k)
		if field != nil {
			if err = field.Set(reflectValue, v); err != nil {
				return err
			}
		}
	}
	return nil
}
