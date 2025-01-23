package cosmo

import (
	"fmt"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
)

const DefaultPageSize = 100

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
	//var err error
	db.statement.Paging = paging
	paging.Init(DefaultPageSize)
	if paging.Rows == nil {
		paging.Rows = []bson.M{}
	}
	tx = db.getInstance()
	stmt := tx.statement
	reflectRows := reflect.ValueOf(paging.Rows)
	indirectRows := reflect.Indirect(reflectRows)
	if indirectRows.Kind() != reflect.Array && indirectRows.Kind() != reflect.Slice {
		_ = tx.Errorf("paging.Rows type not Array or Slice")
		return
	}
	if len(where) > 0 {
		tx = tx.Where(where[0], where[1:]...)
	}
	stmt.value = paging.Rows

	if tx = tx.statement.Parse(); tx.Error != nil {
		return
	}

	if stmt.table == "" {
		_ = tx.Errorf("table not set, please set it like: db.model(&user) or db.table(\"users\") %+v")
		return
	}

	if paging.Update > 0 {
		if f := stmt.schema.LookUpField(DBNameUpdate); f != nil {
			tx.Order(f.DBName, -1)
			tx.Where(fmt.Sprintf("%v > ?", f.DBName), paging.Update)
		}
	}
	//defer tx.reset()

	coll := tx.client.Database(tx.dbname).Collection(stmt.table)
	filter := tx.statement.Clause.Build(stmt.schema)

	if paging.Record == 0 && tx.Error == nil {
		var val int64
		if val, tx.Error = coll.CountDocuments(stmt.Context, filter); tx.Error == nil {
			paging.Result(int(val))
		} else {
			return
		}
	}
	//find
	order := tx.statement.Order()
	opts := options.Find()
	if stmt.Paging.Size > 0 {
		opts.SetLimit(int64(tx.statement.Paging.Size))
	}
	if offset := stmt.Paging.Offset(); offset > 0 {
		opts.SetSkip(int64(offset))
	}
	if len(order) > 0 {
		opts.SetSort(order)
	}
	if projection := tx.statement.selector.Projection(stmt.schema); len(projection) > 0 {
		opts.SetProjection(projection)
	}
	var cursor *mongo.Cursor
	if cursor, tx.Error = coll.Find(stmt.Context, filter, opts); tx.Error != nil {
		return
	}
	//cursor.RemainingBatchLength()
	if reflectRows.Kind() == reflect.Ptr {
		tx.Error = cursor.All(stmt.Context, paging.Rows)
	} else {
		tx.Error = cursor.All(stmt.Context, &paging.Rows)
	}
	if tx.Error == nil {
		tx.RowsAffected = int64(indirectRows.Len())
	}
	return tx
}

// Find  get records that match given conditions
// value must be a pointer to a slice
func (db *DB) Find(val any, where ...any) (tx *DB) {
	tx = db.getInstance()
	if len(where) > 0 {
		tx = db.Where(where[0], where[1:]...)
	}
	tx.statement.value = val
	return tx.callbacks.Query().Execute(tx)
}

// Create insert the value into dbname
func (db *DB) Create(value interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.statement.value = value
	return tx.callbacks.Create().Execute(tx)
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
	tx.statement.value = values
	return tx.callbacks.Update().Execute(tx)
}

// Delete 删除记录
// db.delete(&User{Id:1,name:"myname"})  匹配 _id=1
// db.model(&User).delete(1) 匹配 _id=1
// db.model(&User).delete([]int{1,2,3}) 匹配 _id IN (1,2,3)
// db.model(&User).delete("name = ?","myname") 匹配 name=myname
func (db *DB) Delete(conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx.statement.value = conds[0]
		db.Where(conds[0], conds[1:]...)
	}
	return tx.callbacks.Delete().Execute(tx)
}

// Count 统计文档数,count 必须为一个指向数字的指针  *int *int32 *int64
func (db *DB) Count(count interface{}, conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}

	tx.statement.value = count
	return tx.statement.callbacks.Call(tx, func(db *DB) (err error) {
		var val int64
		coll := tx.client.Database(tx.dbname).Collection(tx.statement.table)
		filter := tx.statement.Clause.Build(db.statement.schema)
		if val, err = coll.CountDocuments(tx.statement.Context, filter); err == nil {
			tx.statement.reflectValue.SetInt(val)
		}
		return err
	})
}
