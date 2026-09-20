package cosmo

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// convertExclusionProjection 将排除式投影(Omit产物)转换为选择式投影
// findAndModify禁止除_id外的排除投影,需要取补集;仅在能枚举schema全字段时转换,
// 否则返回nil保持原样(与旧行为一致)
func convertExclusionProjection(sch *schema.Schema, projection map[string]bool) map[string]bool {
	excluded := false
	for _, v := range projection {
		if !v {
			excluded = true
			break
		}
	}
	if !excluded || sch == nil {
		return nil
	}
	r := map[string]bool{}
	sch.Range(func(field *schema.Field) bool {
		k := field.DBName()
		if v, ok := projection[k]; !ok || v {
			r[k] = true
		}
		return true
	})
	return r
}

// Create insert the value into dbname
func cmdCreate(tx *DB, client *mongo.Client) (err error) {
	coll := client.Database(tx.dbname).Collection(tx.stmt.table)
	switch tx.stmt.reflectValue.Kind() {
	case reflect.Map, reflect.Struct:
		opts := options.InsertOne()
		if _, err = coll.InsertOne(tx.stmt.Context, tx.stmt.value, opts); err == nil {
			tx.RowsAffected = 1
		}
	case reflect.Array, reflect.Slice:
		opts := options.InsertMany()
		var documents []any
		for i := 0; i < tx.stmt.reflectValue.Len(); i++ {
			documents = append(documents, tx.stmt.reflectValue.Index(i).Interface())
		}
		var result *mongo.InsertManyResult
		if result, err = coll.InsertMany(tx.stmt.Context, documents, opts); err == nil {
			tx.RowsAffected = int64(len(result.InsertedIDs))
		}
	default:
		return fmt.Errorf("unsupported create value type:%T", tx.stmt.value)
	}

	return
}

// cmdRange 遍历查询
func cmdRange(tx *DB, client *mongo.Client) (err error) {
	stmt := tx.stmt

	coll := client.Database(tx.dbname).Collection(stmt.table)
	filter := stmt.Clause.Build(stmt.schema)
	if qerr := stmt.Clause.Error(); qerr != nil {
		tx.Errorf(qerr)
		return
	}

	opts := options.Find()
	if stmt.Paging.Size > 0 {
		opts.SetLimit(int64(stmt.Paging.Size))
	}
	if offset := stmt.Paging.Offset(); offset > 0 {
		opts.SetSkip(int64(offset))
	}
	if order := stmt.Order(); len(order) > 0 {
		opts.SetSort(order)
	}
	if projection := stmt.selector.Projection(stmt.schema); len(projection) > 0 {
		opts.SetProjection(projection)
	}

	var cursor *mongo.Cursor
	if cursor, err = coll.Find(stmt.Context, filter, opts); err != nil {
		return
	}
	// 移除defer关闭cursor，因为需要在Range方法中使用cursor进行遍历
	// 将cursor存储在stmt的value字段中，供Range方法使用
	stmt.value = cursor

	return
}

// cmdPage 分页查询
func cmdPage(tx *DB, client *mongo.Client) (err error) {
	stmt := tx.stmt
	paging := stmt.Paging
	if paging == nil {
		return errors.New("paging is nil")
	}

	paging.Init(DefaultPageSize)
	if paging.Rows == nil {
		paging.Rows = []bson.M{}
	}

	if paging.Update > 0 {
		updateFieldName := stmt.pageUpdateField
		if updateFieldName == "" {
			updateFieldName = PageUpdateFieldName
		}
		tx = tx.Order(updateFieldName, -1)
		tx = tx.Where(fmt.Sprintf("%s > ?", updateFieldName), paging.Update)
	}
	reflectRows := reflect.ValueOf(paging.Rows)
	indirectRows := reflect.Indirect(reflectRows)
	if indirectRows.Kind() != reflect.Array && indirectRows.Kind() != reflect.Slice {
		return fmt.Errorf("paging.Rows type not Array or Slice")
	}

	coll := client.Database(tx.dbname).Collection(stmt.table)
	filter := stmt.Clause.Build(stmt.schema)
	if qerr := stmt.Clause.Error(); qerr != nil {
		tx.Errorf(qerr)
		return
	}

	if paging.Record == 0 {
		var val int64
		if val, err = coll.CountDocuments(stmt.Context, filter); err != nil {
			return
		}
		paging.Result(int(val))
	}

	if paging.Page > paging.Total {
		return
	}

	order := stmt.Order()
	opts := options.Find()
	if stmt.Paging.Size > 0 {
		opts.SetLimit(int64(stmt.Paging.Size))
	}
	if offset := stmt.Paging.Offset(); offset > 0 {
		opts.SetSkip(int64(offset))
	}

	if len(order) > 0 {
		opts.SetSort(order)
	}

	if projection := stmt.selector.Projection(stmt.schema); len(projection) > 0 {
		opts.SetProjection(projection)
	}

	var cursor *mongo.Cursor
	if cursor, err = coll.Find(stmt.Context, filter, opts); err != nil {
		return
	}

	if reflectRows.Kind() == reflect.Pointer {
		err = cursor.All(stmt.Context, paging.Rows)
	} else {
		err = cursor.All(stmt.Context, &paging.Rows)
	}

	if err == nil {
		tx.RowsAffected = int64(indirectRows.Len())
	}
	return
}

// Update 通用更新
// map ,BuildUpdate.m 支持 $set $incr $setOnInsert, 其他未使用$字段一律视为$set操作
func cmdUpdate(tx *DB, client *mongo.Client) (err error) {
	stmt := tx.stmt
	var data update.Update
	var upsert bool
	if data, upsert, err = update.BuildWithStmt(stmt); err != nil {
		return
	}
	filter := stmt.Clause.Build(stmt.schema)
	if qerr := stmt.Clause.Error(); qerr != nil {
		tx.Errorf(qerr)
		return
	}
	if len(filter) == 0 {
		return ErrMissingWhereClause
	}
	coll := client.Database(tx.dbname).Collection(stmt.table)
	if stmt.multiple {
		//🔴 upsert 语义对齐 UpdateOne 分支:旧实现从不 SetUpsert,
		// .Upsert().Updates(map) 静默退化为纯更新(匹配 0 行且无任何报错)
		opts := options.UpdateMany()
		if upsert || tx.stmt.upsert {
			opts.SetUpsert(true)
		}
		var result *mongo.UpdateResult
		if result, err = coll.UpdateMany(stmt.Context, filter, data, opts); err == nil {
			tx.RowsAffected = result.ModifiedCount
		}
	} else if stmt.updateAndModifyModel {
		err = findOneAndUpdate(tx, coll, filter, data, upsert)
	} else {
		err = UpdateOne(tx, coll, filter, data, upsert)
	}

	if err != nil {
		tx.Error = NormalizeError(err)
		return
	}
	return
}

func UpdateOne(tx *DB, coll *mongo.Collection, filter clause.Filter, data update.Update, upsert bool) (err error) {
	opts := options.UpdateOne()
	if upsert || tx.stmt.upsert {
		opts.SetUpsert(true)
	}
	var result *mongo.UpdateResult
	if result, err = coll.UpdateOne(tx.stmt.Context, filter, data, opts); err == nil {
		//upsert插入的新文档ModifiedCount=0,计入UpsertedCount,否则调用方无法区分"没匹配"与"新插入"
		tx.RowsAffected = result.ModifiedCount + result.UpsertedCount
	}

	return
}

func findOneAndUpdate(tx *DB, coll *mongo.Collection, filter clause.Filter, data update.Update, upsert bool) (err error) {
	opts := options.FindOneAndUpdate()
	if upsert || tx.stmt.upsert {
		opts.SetUpsert(true)
	}

	if projection := tx.stmt.selector.Projection(tx.stmt.schema); len(projection) > 0 {
		//findAndModify禁止除_id外的排除投影,Omit模式下须转换为选择式(补集)
		if p := convertExclusionProjection(tx.stmt.schema, projection); p != nil {
			projection = p
		}
		opts.SetProjection(projection)
	}
	opts.SetReturnDocument(options.After)
	updateResult := coll.FindOneAndUpdate(tx.stmt.Context, filter, data, opts)
	if err = updateResult.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			err = nil
		}
		return err
	}
	tx.RowsAffected = 1

	//+++ UpdateAndModify 回写改为「整行直接解码进内存模型」。
	// 原路径 Decode(&values) + SetColumn 逐字段反射：值经 map[string]any 中转后，
	// 内嵌子文档（如 Guild.hunt，无 bson tag 的 proto 消息字段）到达字段 setter 时
	// 是扩展 JSON 字符串，schema 无法赋给结构体字段，报
	// `failed to set value {…} to field Hunt`，令含内嵌子文档的文档无法使用 UpdateAndModify
	// （ProjectElf 公会捐献真机复现，2026-09-17）。
	// bson 解码器对「子文档 → 结构体字段」天然支持（与模型加载路径同一套解码），
	// 整行直接解码进 model 后内存与库严格一致，且天然兼容内嵌子文档。
	//
	// ⚠ 本函数仅在 cmdUpdate 的 updateAndModifyModel 分支被调用，所以走到的必然是
	// UpdateAndModify 场景——直接解码进 model 并 return，不再走 values + SetColumn。
	return updateResult.Decode(tx.stmt.model)
}

// cmdDelete delete value match given conditions, if it value has primary key, then will including primary key as condition
func cmdDelete(tx *DB, client *mongo.Client) (err error) {
	filter := tx.stmt.Clause.Build(tx.stmt.schema)
	if qerr := tx.stmt.Clause.Error(); qerr != nil {
		tx.Errorf(qerr)
		return
	}
	if len(filter) == 0 {
		return ErrMissingWhereClause
	}
	coll := client.Database(tx.dbname).Collection(tx.stmt.table)
	var result *mongo.DeleteResult
	if clause.Multiple(filter) {
		result, err = coll.DeleteMany(tx.stmt.Context, filter)
	} else {
		result, err = coll.DeleteOne(tx.stmt.Context, filter)
	}
	if err == nil {
		tx.RowsAffected = result.DeletedCount
	}
	return
}

// cmdQuery find records that match given conditions
// value must be a pointer to a slice
func cmdQuery(tx *DB, client *mongo.Client) (err error) {
	filter := tx.stmt.Clause.Build(tx.stmt.schema)
	if qerr := tx.stmt.Clause.Error(); qerr != nil {
		tx.Errorf(qerr)
		return
	}
	//b, _ := json.Marshal(filter)
	//fmt.Printf("Query Filter:%+v\n", string(b))
	var multiple bool
	switch tx.stmt.reflectValue.Kind() {
	case reflect.Array, reflect.Slice:
		multiple = true
	default:
		multiple = false
	}
	order := tx.stmt.Order()

	coll := client.Database(tx.dbname).Collection(tx.stmt.table)
	if !multiple {
		opts := options.FindOne()
		if offset := tx.stmt.Paging.Offset(); offset > 0 {
			opts.SetSkip(int64(offset))
		}
		if len(order) > 0 {
			opts.SetSort(order)
		}
		if projection := tx.stmt.selector.Projection(tx.stmt.schema); len(projection) > 0 {
			opts.SetProjection(projection)
		}
		result := coll.FindOne(tx.stmt.Context, filter, opts)
		if err = result.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				err = nil
			}
			return
		}
		switch v := tx.stmt.value.(type) {
		case *[]byte:
			*v, err = result.Raw()
		default:
			err = result.Decode(tx.stmt.value)
		}
		if err == nil {
			tx.RowsAffected = 1
		}
	} else {
		opts := options.Find()
		if tx.stmt.Paging.Size > 0 {
			opts.SetLimit(int64(tx.stmt.Paging.Size))
		}
		if offset := tx.stmt.Paging.Offset(); offset > 0 {
			opts.SetSkip(int64(offset))
		}
		if len(order) > 0 {
			opts.SetSort(order)
		}
		if projection := tx.stmt.selector.Projection(tx.stmt.schema); len(projection) > 0 {
			opts.SetProjection(projection)
		}
		var cursor *mongo.Cursor
		if cursor, err = coll.Find(tx.stmt.Context, filter, opts); err != nil {
			return
		}
		if err = cursor.All(tx.stmt.Context, tx.stmt.value); err == nil {
			tx.RowsAffected = int64(tx.stmt.reflectValue.Len())
		}
	}

	return
}

// matchPipeline 在聚合管道最前面拼接$match阶段, filter为空时原样返回
// 通过make+copy生成全新切片, 不修改调用方传入的pipeline(长度、容量与底层数组均不受影响);
// 管道内各阶段文档仅按引用传递, 本函数与驱动都只读取不写入
func matchPipeline(filter clause.Filter, pipeline mongo.Pipeline) mongo.Pipeline {
	if len(filter) == 0 {
		return pipeline
	}
	pipe := make(mongo.Pipeline, 0, len(pipeline)+1)
	pipe = append(pipe, bson.D{{Key: "$match", Value: filter}})
	pipe = append(pipe, pipeline...)
	return pipe
}

// cmdAggregate 聚合查询
// pipeline 为调用方提供的聚合管道, Where条件作为$match阶段拼接在管道最前面
// dest必须为指向切片的指针
func cmdAggregate(tx *DB, client *mongo.Client, pipeline mongo.Pipeline) (err error) {
	stmt := tx.stmt
	// 聚合没有单文档语义, dest必须为指针切片(与cursor.All的要求一致), 提前给出明确错误
	rv := reflect.ValueOf(stmt.value)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("aggregate dest must be a pointer to slice, got %T", stmt.value)
	}
	coll := client.Database(tx.dbname).Collection(stmt.table)
	//🔴 Where 解析失败必须上抛:静默丢条件会让 $match 缺失,聚合范围被放大
	if qerr := stmt.Clause.Error(); qerr != nil {
		tx.Errorf(qerr)
		return
	}
	pipe := matchPipeline(stmt.Clause.Build(stmt.schema), pipeline)
	var cursor *mongo.Cursor
	if cursor, err = coll.Aggregate(stmt.Context, pipe); err != nil {
		return
	}
	// cursor.All会耗尽并关闭cursor, 同时通过原始指针写回结果切片
	if err = cursor.All(stmt.Context, stmt.value); err == nil {
		tx.RowsAffected = int64(stmt.reflectValue.Len())
	}
	return
}
