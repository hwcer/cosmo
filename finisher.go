package cosmo

import "reflect"

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
// values 类型为map ,BuildUpdate.M 时支持 $set $inc $setOnInsert, 其他未使用$前缀字段一律视为$set操作
// values 类型为struct保存所有非零值,如果需要将零值写入数据库，请使用map方式
// 使用Model并且values中未明确设置setOnInsert时,model中除主键和values中明确更新的字段外所有非零值将作为 setOnInsert 值来使用
//db.Update(&User{IID:1,Name:"myname"}) 匹配 _id=1,更新其他非零字段，常用取出对象，修改值,保存
//db.Model(&User{IID:1}).Update(BuildUpdate.M)  匹配 _id=1,更新bson.M中的所有值
//db.Model(&User{}).Where(1).Update(BuildUpdate.M)  匹配 _id=1,更新bson.M中的所有值
//db.Model(&User{}).Where("name = ?","myname").Update(BuildUpdate.M)  匹配 name=myname,更新bson.M中的所有值

func (db *DB) Update(values interface{}, conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.Statement.Dest = values
	return tx.callbacks.Update().Execute(tx)
}

// Delete 删除记录
//db.Delete(&User{IID:1,name:"myname"})  匹配 _id=1
//db.Model(&User).Delete(1) 匹配 _id=1
//db.Model(&User).Delete([]int{1,2,3}) 匹配 _id IN (1,2,3)
//db.Model(&User).Delete("name = ?","myname") 匹配 name=myname
func (db *DB) Delete(value interface{}, conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if reflect.Indirect(reflect.ValueOf(value)).Kind() != reflect.Struct {
		db.Where(value, conds...)
	} else if len(conds) > 0 {
		db.Where(conds[0], conds[1:]...)
	}
	tx.Statement.Dest = value
	return tx.callbacks.Delete().Execute(tx)
}

// Count 统计文档数
func (db *DB) Count(count *int64, conds ...interface{}) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.Statement.Dest = count
	return tx.Statement.callbacks.Call(tx, func(db *DB) (err error) {
		coll := tx.client.Database(tx.dbname).Collection(tx.Statement.Table)
		filter := tx.Statement.Clause.Build(db.Statement.Schema)
		*count, err = coll.CountDocuments(tx.Statement.Context, filter)
		return err
	})
}
