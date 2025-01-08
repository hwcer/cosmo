package cosmo

import (
	"errors"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
)

// Create insert the value into dbname
func cmdCreate(tx *DB) (err error) {
	coll := tx.client.Database(tx.dbname).Collection(tx.statement.table)
	switch tx.statement.reflectValue.Kind() {
	case reflect.Map, reflect.Struct:
		opts := options.InsertOne()
		if _, err = coll.InsertOne(tx.statement.Context, tx.statement.value, opts); err == nil {
			tx.RowsAffected = 1
		}
	case reflect.Array, reflect.Slice:
		opts := options.InsertMany()
		var documents []interface{}
		for i := 0; i < tx.statement.reflectValue.Len(); i++ {
			documents = append(documents, tx.statement.reflectValue.Index(i).Interface())
		}
		var result *mongo.InsertManyResult
		if result, err = coll.InsertMany(tx.statement.Context, documents, opts); err == nil {
			tx.RowsAffected = int64(len(result.InsertedIDs))
		}
	default:
		panic("unhandled default case")
	}
	return
}

// Update 通用更新
// map ,BuildUpdate.m 支持 $set $incr $setOnInsert, 其他未使用$字段一律视为$set操作
// 支持struct 保存所有非零值
func cmdUpdate(tx *DB) (err error) {
	stmt := tx.statement
	var data update.Update
	var upsert bool
	if data, upsert, err = update.Build(stmt.value, stmt.schema, &stmt.selector); err != nil {
		return
	}
	//fmt.Printf("update:%+v\n", update)
	filter := stmt.Clause.Build(stmt.schema)
	//filter := tx.statement.Clause.Build(tx.statement.schema)
	if len(filter) == 0 {
		return ErrMissingWhereClause
	}
	//fmt.Printf("Update filter:%+v\n", filter)
	coll := tx.client.Database(tx.dbname).Collection(stmt.table)
	//reflectModel := reflect.Indirect(reflect.ValueOf(tx.statement.model))
	if stmt.multiple {
		opts := options.Update()
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
	opts := options.Update()
	if upsert || tx.statement.upsert {
		opts.SetUpsert(true)
	}
	var result *mongo.UpdateResult
	if result, err = coll.UpdateOne(tx.statement.Context, filter, data, opts); err == nil {
		tx.RowsAffected = result.MatchedCount
	}

	return
}

func findOneAndUpdate(tx *DB, coll *mongo.Collection, filter clause.Filter, data update.Update, upsert bool) (err error) {
	opts := options.FindOneAndUpdate()
	if upsert || tx.statement.upsert {
		opts.SetUpsert(true)
	}

	if projection := tx.statement.selector.Projection(tx.statement.schema); len(projection) > 0 {
		opts.SetProjection(projection)
	}
	opts.SetReturnDocument(options.After)
	values := make(map[string]any)
	updateResult := coll.FindOneAndUpdate(tx.statement.Context, filter, data, opts)
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

// cmdDelete delete value match given conditions, if the value has primary key, then will including the primary key as condition
func cmdDelete(tx *DB) (err error) {
	filter := tx.statement.Clause.Build(tx.statement.schema)
	if len(filter) == 0 {
		return ErrMissingWhereClause
	}
	coll := tx.client.Database(tx.dbname).Collection(tx.statement.table)
	var result *mongo.DeleteResult
	if clause.Multiple(filter) {
		result, err = coll.DeleteMany(tx.statement.Context, filter)
	} else {
		result, err = coll.DeleteOne(tx.statement.Context, filter)
	}
	if err == nil {
		tx.RowsAffected = result.DeletedCount
	}
	return
}

// cmdQuery find records that match given conditions
// value must be a pointer to a slice
func cmdQuery(tx *DB) (err error) {
	filter := tx.statement.Clause.Build(tx.statement.schema)
	//b, _ := json.Marshal(filter)
	//fmt.Printf("Query Filter:%+v\n", string(b))
	var multiple bool
	switch tx.statement.reflectValue.Kind() {
	case reflect.Array, reflect.Slice:
		multiple = true
	default:
		multiple = false
	}
	order := tx.statement.Order()

	coll := tx.client.Database(tx.dbname).Collection(tx.statement.table)
	if !multiple {
		opts := options.FindOne()
		if offset := tx.statement.Paging.Offset(); offset > 0 {
			opts.SetSkip(int64(offset))
		}
		if len(order) > 0 {
			opts.SetSort(order)
		}
		if projection := tx.statement.selector.Projection(tx.statement.schema); len(projection) > 0 {
			opts.SetProjection(projection)
		}
		result := coll.FindOne(tx.statement.Context, filter, opts)
		if err = result.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				err = nil
			}
			return
		}
		switch v := tx.statement.value.(type) {
		case *[]byte:
			*v, err = result.DecodeBytes()
		default:
			err = result.Decode(tx.statement.value)
		}
		if err == nil {
			tx.RowsAffected = 1
		}
	} else {
		opts := options.Find()
		if tx.statement.Paging.Size > 0 {
			opts.SetLimit(int64(tx.statement.Paging.Size))
		}
		if offset := tx.statement.Paging.Offset(); offset > 0 {
			opts.SetSkip(int64(offset))
		}
		if len(order) > 0 {
			opts.SetSort(order)
		}
		if projection := tx.statement.selector.Projection(tx.statement.schema); len(projection) > 0 {
			opts.SetProjection(projection)
		}
		var cursor *mongo.Cursor
		if cursor, err = coll.Find(tx.statement.Context, filter, opts); err != nil {
			return
		}
		if err = cursor.All(tx.statement.Context, tx.statement.value); err == nil {
			tx.RowsAffected = int64(tx.statement.reflectValue.Len())
		}
	}

	return
}
