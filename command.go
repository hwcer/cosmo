package cosmo

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
		var documents []interface{}
		for i := 0; i < tx.stmt.reflectValue.Len(); i++ {
			documents = append(documents, tx.stmt.reflectValue.Index(i).Interface())
		}
		var result *mongo.InsertManyResult
		if result, err = coll.InsertMany(tx.stmt.Context, documents, opts); err == nil {
			tx.RowsAffected = int64(len(result.InsertedIDs))
		}
	default:
		panic("unhandled default case")
	}

	return
}

// cmdRange 遍历查询
func cmdRange(tx *DB, client *mongo.Client) (err error) {
	stmt := tx.stmt

	coll := client.Database(tx.dbname).Collection(stmt.table)
	filter := stmt.Clause.Build(stmt.schema)

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

	if paging.Record == 0 {
		var val int64
		if val, err = coll.CountDocuments(stmt.Context, filter); err != nil {
			return
		}
		paging.Result(int(val))
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

	if reflectRows.Kind() == reflect.Ptr {
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
	if len(filter) == 0 {
		return ErrMissingWhereClause
	}
	coll := client.Database(tx.dbname).Collection(stmt.table)
	if stmt.multiple {
		opts := options.UpdateMany()
		var result *mongo.UpdateResult
		if result, err = coll.UpdateMany(stmt.Context, filter, data, opts); err == nil {
			tx.RowsAffected = result.MatchedCount
		}
	} else if stmt.updateAndModifyModel {
		err = findOneAndUpdate(tx, coll, filter, data, upsert)
	} else {
		err = UpdateOne(tx, coll, filter, data, upsert)
	}

	if err != nil {
		tx.Error = err
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
		tx.RowsAffected = result.MatchedCount
	}

	return
}

func findOneAndUpdate(tx *DB, coll *mongo.Collection, filter clause.Filter, data update.Update, upsert bool) (err error) {
	opts := options.FindOneAndUpdate()
	if upsert || tx.stmt.upsert {
		opts.SetUpsert(true)
	}

	if projection := tx.stmt.selector.Projection(tx.stmt.schema); len(projection) > 0 {
		opts.SetProjection(projection)
	}
	opts.SetReturnDocument(options.After)
	values := make(map[string]any)
	updateResult := coll.FindOneAndUpdate(tx.stmt.Context, filter, data, opts)
	if err = updateResult.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			err = nil
		}
		return err
	}

	tx.RowsAffected = 1
	err = updateResult.Decode(&values)
	if len(values) > 0 {
		_ = tx.SetColumn(values)
	}
	return
}

// cmdDelete delete value match given conditions, if it value has primary key, then will including primary key as condition
func cmdDelete(tx *DB, client *mongo.Client) (err error) {
	filter := tx.stmt.Clause.Build(tx.stmt.schema)
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
