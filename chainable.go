package cosmo

import (
	"fmt"
	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"go.mongodb.org/mongo-driver/bson"
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
	tx.stmt.model = value
	if len(modify) > 0 && modify[0] {
		tx.stmt.updateAndModifyModel = true
	}
	return
}

// table specify the table you would like to run db operations
// 使用TABLE 时select,order,Omit 中必须使用数据库字段名称

func (db *DB) Table(name string) (tx *DB) {
	tx = db.getInstance()
	tx.stmt.table = name
	return
}

// Upsert update时如果不存在自动insert
func (db *DB) Upsert() (tx *DB) {
	tx = db.getInstance()
	tx.stmt.upsert = true
	return
}

// Multiple 强制批量更新
func (db *DB) Multiple() (tx *DB) {
	tx = db.getInstance()
	tx.stmt.multiple = true
	return
}

// Omit specify fields that you want to ignore when creating, updating and querying
func (db *DB) Omit(columns ...string) (tx *DB) {
	tx = db.getInstance()
	if !tx.stmt.selector.Omit(columns...) {
		tx.Error = ErrOmitOnSelectsExist
	}
	return
}

// Select specify fields that you want when querying, creating, updating
func (db *DB) Select(columns ...string) (tx *DB) {
	tx = db.getInstance()
	if !tx.stmt.selector.Select(columns...) {
		tx.Error = ErrSelectOnOmitsExist
	}
	return
}

// Where 查询条件
// 参考 query包
func (db *DB) Where(query interface{}, args ...interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.stmt.Clause.Where(query, args...)
	return
}

// Order specify order when retrieve records from dbname
// Order 排序方式 1 和 -1 来指定排序的方式，其中 1 为升序排列，而 -1 是用于降序排列。
func (db *DB) Order(key string, value int) (tx *DB) {
	tx = db.getInstance()
	if value > 0 {
		value = 1
	} else {
		value = -1
	}
	tx.stmt.orders = append(tx.stmt.orders, bson.E{
		Key: key, Value: value,
	})
	return
}

func (db *DB) Limit(limit int) (tx *DB) {
	tx = db.getInstance()
	tx.stmt.Paging.Size = limit
	return
}

// SetColumn set column's value to model
//
//	stmt.SetColumn("Name", "jinzhu") // Hooks Method
func (db *DB) SetColumn(data map[string]interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()
	if db.stmt.model == nil {
		return nil
	}
	sch, err := schema.Parse(db.stmt.model)
	if err != nil {
		return err
	}
	reflectValue := reflect.ValueOf(db.stmt.model)
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
