package cosmo

import (
	"github.com/hwcer/cosgo/values"
	"github.com/hwcer/cosmo/clause"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
)

const DefaultPageSize = 100

func (tx *DB) reset() {
	tx.Statement.Model = nil
	tx.Statement.Schema = nil
	tx.Statement.Paging = &Paging{}
	tx.Statement.Clause = clause.New()
	tx.Statement.multiple = false
}

//View 分页查询
func (db *DB) View(paging *values.Paging, conds ...interface{}) (tx *DB) {
	var err error
	if paging.Page == 0 || paging.Size == 0 {
		paging.Init(DefaultPageSize)
	}
	if paging.Rows == nil {
		paging.Rows = []bson.M{}
	}
	reflectRows := reflect.Indirect(reflect.ValueOf(paging.Rows))
	if reflectRows.Kind() != reflect.Array && reflectRows.Kind() != reflect.Slice {
		tx.Errorf("paging.Rows type not Array or Slice")
	}
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = db.Where(conds[0], conds[1:]...)
	}
	tx.Page(paging.Page, paging.Size)
	tx.Statement.Dest = paging.Rows
	tx = db.Statement.Parse()
	if tx.Error != nil {
		return
	}

	stmt := tx.Statement
	if stmt.Table == "" {
		tx.Errorf("Table not set, please set it like: db.Model(&user) or db.Table(\"users\") %+v")
		return
	}
	defer tx.reset()

	coll := tx.client.Database(tx.dbname).Collection(stmt.Table)
	filter := tx.Statement.Clause.Build(stmt.Schema)

	if paging.Record == 0 {
		var val int64
		if val, err = coll.CountDocuments(stmt.Context, filter); err == nil {
			paging.Record = int(val)
		}
	}
	paging.Total = paging.Record / paging.Size
	if paging.Record%paging.Size > 0 {
		paging.Total += 1
	}

	//find
	var (
		order      bson.D
		projection bson.M
	)
	if projection, order, err = tx.Statement.projection(); err != nil {
		return
	}
	opts := options.Find()
	if stmt.Paging.limit > 0 {
		opts.SetLimit(int64(tx.Statement.Paging.limit))
	}
	if stmt.Paging.offset > 0 {
		opts.SetSkip(int64(tx.Statement.Paging.offset))
	}
	if len(order) > 0 {
		opts.SetSort(order)
	}
	if len(projection) > 0 {
		opts.SetProjection(projection)
	}
	var cursor *mongo.Cursor
	if cursor, err = coll.Find(stmt.Context, filter, opts); err != nil {
		return
	}
	cursor.RemainingBatchLength()
	if err = cursor.All(stmt.Context, &paging.Rows); err == nil {
		tx.RowsAffected = int64(reflectRows.Len())
	}
	return tx
}

//Find find records that match given conditions
//dest must be a pointer to a slice
func (db *DB) Find(dest interface{}, conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = db.Where(conds[0], conds[1:]...)
	}
	tx.Statement.Dest = dest
	return tx.callbacks.Query().Execute(tx)
}

// Create insert the value into dbname
func (db *DB) Create(value interface{}) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Dest = value
	return tx.callbacks.Create().Execute(tx)
}

//Update 通用更新
// values 类型为map ,bson.M 时支持 $set $inc $setOnInsert, 其他未使用$前缀字段一律视为$set操作
// values 类型为struct保存所有非零值,如果需要将零值写入数据库，请使用map方式
//db.Update(&User{Id:1,Name:"myname"},1) 匹配 _id=1,更新其他非零字段，常用取出对象，修改值,保存
//db.Model(&User{}).Update(bson.M,1)  匹配 _id=1,更新bson.M中的所有值
//db.Model(&User{}).Where(1).Update(bson.M)  匹配 _id=1,更新bson.M中的所有值
//db.Model(&User{}).Where("name = ?","myname").Update(bson.M)  匹配 name=myname,更新bson.M中的所有值

func (db *DB) Update(values interface{}, conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.Statement.Dest = values
	return tx.callbacks.Update().Execute(tx)
}

// Delete 删除记录
//db.Delete(&User{Id:1,name:"myname"})  匹配 _id=1
//db.Model(&User).Delete(1) 匹配 _id=1
//db.Model(&User).Delete([]int{1,2,3}) 匹配 _id IN (1,2,3)
//db.Model(&User).Delete("name = ?","myname") 匹配 name=myname
func (db *DB) Delete(conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx.Statement.Dest = conds[0]
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

	tx.Statement.Dest = count
	return tx.Statement.callbacks.Call(tx, func(db *DB) (err error) {
		var val int64
		coll := tx.client.Database(tx.dbname).Collection(tx.Statement.Table)
		filter := tx.Statement.Clause.Build(db.Statement.Schema)
		if val, err = coll.CountDocuments(tx.Statement.Context, filter); err == nil {
			tx.Statement.ReflectValue.SetInt(val)
		}
		return err
	})
}
