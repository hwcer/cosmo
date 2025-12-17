package cosmo

import (
	"errors"
	"reflect"

	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/mongo"
)

const DefaultPageSize = 1000

func (db *DB) Set(key string, val any) (tx *DB) {
	up := update.Update{}
	up.Set(key, val)
	return db.Update(up)
}
func (db *DB) Inc(key string, val int) (tx *DB) {
	up := update.Update{}
	up.Inc(key, val)
	return db.Update(up)
}

// Page 分页查询
func (db *DB) Page(paging *Paging, where ...any) (tx *DB) {
	// 先获取新实例
	tx = db.getInstance()
	// 对新实例进行修改
	tx.stmt.Paging = paging
	tx.stmt.value = paging.Rows
	if len(where) > 0 {
		tx = tx.Where(where[0], where[1:]...)
	}
	// 使用回调机制执行cmdPage命令
	tx = tx.callbacks.Call(tx, cmdPage)
	return tx
}

// Range 遍历
func (db *DB) Range(f func(Cursor) bool) (tx *DB) {
	// 先获取新实例
	tx = db.getInstance()
	// 使用回调机制执行cmdRange命令
	tx = tx.callbacks.Call(tx, cmdRange, func(db *DB) error {
		cursor, ok := tx.stmt.value.(*mongo.Cursor)
		if !ok || cursor == nil {
			return errors.New("cursor is nil")
		}
		defer cursor.Close(tx.stmt.Context)

		for cursor.Next(tx.stmt.Context) {
			if !f(cursor) {
				break
			}
		}
		if err := cursor.Err(); err != nil {
			tx.Error = err
		}
		return nil
	})

	return tx
}

// Query  get records that match given conditions
// value must be a pointer to a slice
func (db *DB) Query(val any, where ...any) (tx *DB) {
	tx = db.getInstance()
	if len(where) > 0 {
		tx = db.Where(where[0], where[1:]...)
	}
	tx.stmt.value = val
	return tx.callbacks.Query().Execute(tx)
}

// Find  仅仅满足 GORM习惯
func (db *DB) Find(val any, where ...any) (tx *DB) {
	return db.Query(val, where...)
}

// First  获取第一条记录（主键升序）
func (db *DB) First(val any, where ...any) (tx *DB) {
	tx = db.getInstance()
	if len(where) > 0 {
		tx = db.Where(where[0], where[1:]...)
	}
	tx.Limit(1)
	if len(tx.stmt.orders) == 0 {
		tx = tx.Order("_id", 1)
	}
	tx.stmt.value = val
	return tx.callbacks.Query().Execute(tx)
}

// Last 获取最后一条记录（主键降序）
func (db *DB) Last(val any, where ...any) (tx *DB) {
	tx = db.getInstance()
	if len(where) > 0 {
		tx = db.Where(where[0], where[1:]...)
	}
	tx.Limit(1)
	if len(tx.stmt.orders) == 0 {
		tx = tx.Order("_id", 1)
	}
	tx.stmt.value = val
	return tx.callbacks.Query().Execute(tx)
}

// Take  获取一条记录，没有指定排序字段
func (db *DB) Take(val any, where ...any) (tx *DB) {
	tx = db.getInstance()
	if len(where) > 0 {
		tx = db.Where(where[0], where[1:]...)
	}
	tx.Limit(1)
	tx.stmt.value = val
	return tx.callbacks.Query().Execute(tx)
}

// Create insert the value into dbname
func (db *DB) Create(value interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.stmt.value = value
	return tx.callbacks.Create().Execute(tx)
}

func (db *DB) Save(values any, conds ...any) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.stmt.value = values
	tx.stmt.includeZeroValue = true
	return tx.callbacks.Update().Execute(tx)
}

//Update 通用更新
// values 类型为map ,bson.M 时支持 $set $inc $setOnInsert, 其他未使用$前缀字段一律视为$set操作
// values 类型为struct保存所有非零值,如果需要将零值写入数据库，请使用map方式
//db.Update(&User{Id:1,Name:"myname"},1) 匹配 _id=1,更新其他非零字段，常用取出对象，修改值,保存
//db.model(&User{}).Update(bson.M,1)  匹配 _id=1,更新bson.M中的所有值
//db.model(&User{}).Where(1).Update(bson.M)  匹配 _id=1,更新bson.M中的所有值
//db.model(&User{}).Where("name = ?","myname").Update(bson.M)  匹配 name=myname,更新bson.M中的所有值

func (db *DB) Update(values any, conds ...any) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.stmt.value = values
	return tx.callbacks.Update().Execute(tx)
}

// Updates 更新多列
// Updates 方法支持 struct 和 map[string]interface{} 参数。当使用 struct 更新时，默认情况下只会更新非零值的字段
// 如果您想要在更新时选择、忽略某些字段，您可以使用 Select、Omit
// 自动关闭 updateAndModify
func (db *DB) Updates(values any, conds ...any) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.stmt.value = values
	tx.stmt.multiple = true
	tx.stmt.updateAndModifyModel = false
	return tx.callbacks.Update().Execute(tx)
}

// Delete 删除记录
// db.model(&User).delete(1) 匹配 _id=1
// db.model(&User).delete([]int{1,2,3}) 匹配 _id IN (1,2,3)
// db.model(&User).delete("name = ?","myname") 匹配 name=myname
// db.delete(&User{Id:1}) 根据结构体中的_id字段删除记录
func (db *DB) Delete(conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		// 检查第一个参数是否为结构体或指针，设置为model以解析表名
		val := conds[0]
		valType := reflect.TypeOf(val)
		if valType != nil {
			if valType.Kind() == reflect.Ptr {
				valType = valType.Elem()
			}
			if valType.Kind() == reflect.Struct {
				tx.stmt.model = val
			} else {
				tx = tx.Where(conds[0], conds[1:]...)
			}
		} else {
			tx = tx.Where(conds[0], conds[1:]...)
		}
	}
	return tx.callbacks.Delete().Execute(tx)
}

// Count 统计文档数,count 必须为一个指向数字的指针  *int *int32 *int64
func (db *DB) Count(count interface{}, conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.stmt.value = count
	return tx.stmt.callbacks.Call(tx, func(db *DB, client *mongo.Client) (err error) {
		var val int64
		coll := client.Database(tx.dbname).Collection(tx.stmt.table)
		filter := tx.stmt.Clause.Build(db.stmt.schema)
		if val, err = coll.CountDocuments(tx.stmt.Context, filter); err == nil {
			tx.stmt.reflectValue.SetInt(val)
		}
		return err
	})
}
