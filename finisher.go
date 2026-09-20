package cosmo

import (
	"errors"
	"reflect"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/v2/mongo"
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
			tx.Errorf(err)
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
		tx = tx.Order("_id", 1) //1为升序,取最小_id
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
		tx = tx.Order("_id", -1) //-1为降序,取最大_id
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
func (db *DB) Create(value any) (tx *DB) {
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
// Updates 方法支持 struct 和 map[string]any 参数。当使用 struct 更新时，默认情况下只会更新非零值的字段
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
// db.delete(&User{Id:1}) 根据结构体中的_id字段删除记录,仅仅在不包含任何条件时才解析
func (db *DB) Delete(conds ...any) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		// 检查第一个参数是否为结构体或指针，设置为model以解析表名
		val := conds[0]
		valType := reflect.TypeOf(val)
		if valType == nil {
			tx.Errorf("cannot delete nil value")
			return
		}
		if valType.Kind() == reflect.Pointer {
			valType = valType.Elem()
		}
		if valType.Kind() == reflect.Struct {
			if tx.stmt.model == nil {
				tx.stmt.model = val
			}
			if len(conds) > 1 {
				tx = tx.Where(conds[1], conds[2:]...)
			}
			// db.delete(&User{Id:1}) 按结构体主键删除(文档声明的行为):
			// 主键非零条件为空时生成_id查询条件,否则cmdDelete会因filter为空而报错
			if tx.stmt.Clause.Len() == 0 {
				if sch, perr := schema.Parse(val); perr == nil {
					if field := sch.LookUpField(clause.MongoPrimaryName); field != nil {
						rv := reflect.Indirect(reflect.ValueOf(val))
						if v := rv.FieldByIndex(field.StructField.Index); v.IsValid() && !v.IsZero() {
							tx = tx.Where(clause.MongoPrimaryName, v.Interface())
						}
					}
				}
			}
		} else {
			tx = tx.Where(conds[0], conds[1:]...)
		}

	}
	return tx.callbacks.Delete().Execute(tx)
}

// Count 统计文档数,count 必须为一个指向数字的指针  *int *int32 *int64
func (db *DB) Count(count any, conds ...any) (tx *DB) {
	tx = db.getInstance()
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.stmt.value = count
	return tx.stmt.callbacks.Call(tx, func(db *DB, client *mongo.Client) (err error) {
		var val int64
		//🔴 Where 解析失败必须上抛:静默丢条件会把 Count 放大成全表统计
		filter := tx.stmt.Clause.Build(db.stmt.schema)
		if qerr := db.stmt.Clause.Error(); qerr != nil {
			db.Errorf(qerr)
			return qerr
		}
		coll := client.Database(tx.dbname).Collection(tx.stmt.table)
		if val, err = coll.CountDocuments(tx.stmt.Context, filter); err == nil {
			tx.stmt.reflectValue.SetInt(val)
		}
		return err
	})
}

// Aggregate 聚合查询
// pipeline 为MongoDB聚合管道(mongo.Pipeline), Where等链式条件将作为$match阶段拼接在管道最前面,
// 即基于聚合前的原始文档过滤(同SQL中GROUP BY前的WHERE); HAVING语义请直接在pipeline中追加$match
//
// dest 接收聚合结果集, 必须为【指向切片的指针】, 聚合输出的每个结果文档解码为切片的一个元素;
// 切片元素类型按聚合末阶段的输出键自定义结构体, bson标签与输出键一一对应即可:
//
//	// 1) 按分组键分组求和: 输出文档为 {_id:分组键, sum:聚合值}, 每个分组一行
//	var rows []struct {
//	    Group string `bson:"_id"` // $group 的 _id 即分组键
//	    Sum   int64  `bson:"sum"` // $group 里别名 sum 的 $sum 表达式
//	}
//	pipeline := mongo.Pipeline{bson.D{
//	    {Key: "$group", Value: bson.D{
//	        {Key: "_id", Value: "$guild"},                       // 分组键
//	        {Key: "sum", Value: bson.D{{Key: "$sum", Value: "$power"}}}, // 求和别名
//	    }},
//	}}
//	db.Model(&GuildMember{}).Aggregate(&rows, pipeline)
//
//	// 2) 无分组全局求和: $group 的 _id 固定传 nil, 整个集合只产出一行(空集合时切片为空)
//	var rows []struct{ Sum int64 `bson:"sum"` }
//	pipeline := mongo.Pipeline{bson.D{
//	    {Key: "$group", Value: bson.D{
//	        {Key: "_id", Value: nil},
//	        {Key: "sum", Value: bson.D{{Key: "$sum", Value: "$power"}}},
//	    }},
//	}}
//
//	// 3) 计数: $group _id:nil + $sum:1, 输出 {count:N}
//	var rows []struct{ Count int64 `bson:"count"` }
//
// 对应规则: 输出键与结构体字段按bson标签匹配(顺序无关), 输出中多出的键忽略, 结构体缺失的键保持零值;
// dest 与 Model 类型无关, 不要复用模型结构体来接
// Order/Limit/Select/Page 等链式方法不参与聚合, 排序/截断/投影请在pipeline中使用$sort/$limit/$project
// 必须先通过Model或Table指定集合, dest无法用于解析集合
// db.Model(&GuildMember{}).Where("guild = ?", gid).Aggregate(&rows, pipeline)
// db.Table("guild_member").Aggregate(&rows, pipeline, "guild = ?", gid)
func (db *DB) Aggregate(dest any, pipeline mongo.Pipeline, conds ...any) (tx *DB) {
	tx = db.getInstance()
	// dest是聚合结果集而非模型行, 无法像Query那样回退用value解析集合,
	// 缺少Model/Table时直接报错, 防止静默聚合到错误的集合上
	if tx.stmt.model == nil && tx.stmt.table == "" {
		return tx.Errorf("aggregate requires Model or Table to resolve collection")
	}
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	tx.stmt.value = dest
	return tx.stmt.callbacks.Call(tx, func(db *DB, client *mongo.Client) (err error) {
		return cmdAggregate(tx, client, pipeline)
	})
}
