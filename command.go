package cosmo

import (
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
)

// Create insert the value into dbname
func cmdCreate(tx *DB) (err error) {
	coll := tx.client.Database(tx.dbname).Collection(tx.Statement.Table)
	switch tx.Statement.ReflectValue.Kind() {
	case reflect.Map, reflect.Struct:
		opts := options.InsertOne()
		if _, err = coll.InsertOne(tx.Statement.Context, tx.Statement.Dest, opts); err == nil {
			tx.RowsAffected = 1
		}
	case reflect.Array, reflect.Slice:
		opts := options.InsertMany()
		var documents []interface{}
		for i := 0; i < tx.Statement.ReflectValue.Len(); i++ {
			documents = append(documents, tx.Statement.ReflectValue.Index(i).Interface())
		}
		var result *mongo.InsertManyResult
		if result, err = coll.InsertMany(tx.Statement.Context, documents, opts); err == nil {
			tx.RowsAffected = int64(len(result.InsertedIDs))
		}
	}
	return
}

// Update 通用更新
// map ,BuildUpdate.m 支持 $set $incr $setOnInsert, 其他未使用$字段一律视为$set操作
// 支持struct 保存所有非零值
func cmdUpdate(tx *DB) (err error) {
	stmt := tx.Statement
	var data update.Update
	if data, err = update.Build(stmt.Dest, stmt.schema, &stmt.Selector); err != nil {
		return
	}
	//fmt.Printf("update:%+v\n", update)
	filter := stmt.Clause.Build(stmt.schema)
	//filter := tx.Statement.Clause.Build(tx.Statement.schema)
	if len(filter) == 0 {
		return ErrMissingWhereClause
	}
	//fmt.Printf("Update filter:%+v\n", filter)
	coll := tx.client.Database(tx.dbname).Collection(stmt.Table)
	//reflectModel := reflect.Indirect(reflect.ValueOf(tx.Statement.Model))
	if stmt.multiple {
		opts := options.Update()
		var result *mongo.UpdateResult
		if result, err = coll.UpdateMany(stmt.Context, filter, data, opts); err == nil {
			tx.RowsAffected = result.MatchedCount
		}
	} else if stmt.HasValidModel() {
		if projection := stmt.Selector.Projection(stmt.schema); len(projection) == 0 {
			err = UpdateOne(tx, coll, filter, data)
		} else {
			err = findOneAndUpdate(tx, coll, filter, data, projection)
		}
	} else {
		err = UpdateOne(tx, coll, filter, data)
	}

	if err != nil {
		tx.Error = err
		return
	}
	return
}

func UpdateOne(tx *DB, coll *mongo.Collection, filter clause.Filter, data update.Update) (err error) {
	opts := options.Update()
	if _, ok := data[MongoSetOnInsert]; ok || tx.Statement.upsert {
		opts.SetUpsert(true)
	}
	var result *mongo.UpdateResult
	if result, err = coll.UpdateOne(tx.Statement.Context, filter, data, opts); err == nil {
		tx.RowsAffected = result.MatchedCount
	}

	return
}

func findOneAndUpdate(tx *DB, coll *mongo.Collection, filter clause.Filter, data update.Update, projection map[string]int) (err error) {
	opts := options.FindOneAndUpdate()
	if _, ok := data[MongoSetOnInsert]; ok || tx.Statement.upsert {
		opts.SetUpsert(true)
	}
	opts.SetProjection(projection)
	opts.SetReturnDocument(options.After)
	values := make(map[string]interface{})
	updateResult := coll.FindOneAndUpdate(tx.Statement.Context, filter, data, opts)
	if err = updateResult.Err(); err != nil {
		if err == mongo.ErrNoDocuments {
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

// delete delete value match given conditions, if the value has primary key, then will including the primary key as condition
func cmdDelete(tx *DB) (err error) {
	filter := tx.Statement.Clause.Build(tx.Statement.schema)
	if len(filter) == 0 {
		return ErrMissingWhereClause
	}
	coll := tx.client.Database(tx.dbname).Collection(tx.Statement.Table)
	var result *mongo.DeleteResult
	if clause.Multiple(filter) {
		result, err = coll.DeleteMany(tx.Statement.Context, filter)
	} else {
		result, err = coll.DeleteOne(tx.Statement.Context, filter)
	}
	if err == nil {
		tx.RowsAffected = result.DeletedCount
	}
	return
}

// Find find records that match given conditions
// dest must be a pointer to a slice
func cmdQuery(tx *DB) (err error) {
	filter := tx.Statement.Clause.Build(tx.Statement.schema)
	//b, _ := json.Marshal(filter)
	//fmt.Printf("Query Filter:%+v\n", string(b))
	var multiple bool
	switch tx.Statement.ReflectValue.Kind() {
	case reflect.Array, reflect.Slice:
		multiple = true
	default:
		multiple = false
	}
	order := tx.Statement.Order()

	coll := tx.client.Database(tx.dbname).Collection(tx.Statement.Table)
	if !multiple {
		opts := options.FindOne()
		if tx.Statement.paging.offset > 0 {
			opts.SetSkip(int64(tx.Statement.paging.offset))
		}
		if len(order) > 0 {
			opts.SetSort(order)
		}
		if projection := tx.Statement.Selector.Projection(nil); len(projection) > 0 {
			opts.SetProjection(projection)
		}
		result := coll.FindOne(tx.Statement.Context, filter, opts)
		if err = result.Err(); err != nil {
			if err == mongo.ErrNoDocuments {
				err = nil
			}
			return
		}
		switch v := tx.Statement.Dest.(type) {
		case *[]byte:
			*v, err = result.DecodeBytes()
		default:
			err = result.Decode(tx.Statement.Dest)
		}
		if err == nil {
			tx.RowsAffected = 1
		}
	} else {
		opts := options.Find()
		if tx.Statement.paging.limit > 0 {
			opts.SetLimit(int64(tx.Statement.paging.limit))
		}
		if tx.Statement.paging.offset > 0 {
			opts.SetSkip(int64(tx.Statement.paging.offset))
		}
		if len(order) > 0 {
			opts.SetSort(order)
		}
		if projection := tx.Statement.Selector.Projection(nil); len(projection) > 0 {
			opts.SetProjection(projection)
		}
		var cursor *mongo.Cursor
		if cursor, err = coll.Find(tx.Statement.Context, filter, opts); err != nil {
			return
		}
		cursor.RemainingBatchLength()
		if err = cursor.All(tx.Statement.Context, tx.Statement.Dest); err == nil {
			tx.RowsAffected = int64(tx.Statement.ReflectValue.Len())
		}
	}

	return
}
