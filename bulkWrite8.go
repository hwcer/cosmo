package cosmo

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// BulkWrite8 跨集合批量写入，需要 MongoDB 8.0+
// 通过 client.BulkWrite() 实现，支持多个集合的操作在一次请求中原子提交
type BulkWrite8 struct {
	tx      *DB
	ctx     context.Context
	writes  []mongo.ClientBulkWrite
	opts    []options.Lister[options.ClientBulkWriteOptions]
	result  *mongo.ClientBulkWriteResult
	schemas map[reflect.Type]*bulkWrite8Schema
	Error   error
}

type bulkWrite8Schema struct {
	table  string
	schema *schema.Schema
}

func (bw8 *BulkWrite8) resolve(model any) (string, *schema.Schema, error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if info, ok := bw8.schemas[t]; ok {
		return info.table, info.schema, nil
	}
	sch, err := schema.Parse(model)
	if err != nil {
		return "", nil, err
	}
	bw8.schemas[t] = &bulkWrite8Schema{table: sch.Table, schema: sch}
	return sch.Table, sch, nil
}

// Size 等待提交的操作数量
func (bw8 *BulkWrite8) Size() int {
	return len(bw8.writes)
}

func (bw8 *BulkWrite8) update(model any, data any, where []any, includeZeroValue bool) {
	table, sch, err := bw8.resolve(model)
	if err != nil {
		bw8.Error = err
		return
	}
	query := clause.New()
	query.Where(where[0], where[1:]...)
	value, upsert, err := update.Build(data, sch, nil, includeZeroValue)
	if err != nil {
		bw8.Error = err
		return
	}
	if f, ok := model.(ModelBulkWriteFilter); ok {
		f.BulkWriteFilter(value)
	}
	m := mongo.NewClientUpdateOneModel().SetFilter(query.Build(sch)).SetUpdate(value)
	if upsert {
		m.SetUpsert(true)
	}
	bw8.writes = append(bw8.writes, mongo.ClientBulkWrite{
		Database: bw8.tx.dbname, Collection: table, Model: m,
	})
}

// Update 添加更新操作（跳过零值字段）
func (bw8 *BulkWrite8) Update(model any, data any, where ...any) {
	bw8.update(model, data, where, false)
}

// Save 添加保存操作（包含零值字段）
func (bw8 *BulkWrite8) Save(model any, data any, where ...any) {
	bw8.update(model, data, where, true)
}

// Unset 移除文档字段（$unset操作）
func (bw8 *BulkWrite8) Unset(model any, keys []string, where ...any) {
	table, sch, err := bw8.resolve(model)
	if err != nil {
		bw8.Error = err
		return
	}
	query := clause.New()
	query.Where(where[0], where[1:]...)
	up := update.New()
	for _, k := range keys {
		up.Unset(k)
	}
	m := mongo.NewClientUpdateOneModel().SetFilter(query.Build(sch)).SetUpdate(up)
	bw8.writes = append(bw8.writes, mongo.ClientBulkWrite{
		Database: bw8.tx.dbname, Collection: table, Model: m,
	})
}

// Insert 添加插入操作
func (bw8 *BulkWrite8) Insert(model any, documents ...any) {
	table, _, err := bw8.resolve(model)
	if err != nil {
		bw8.Error = err
		return
	}
	for _, doc := range documents {
		m := mongo.NewClientInsertOneModel().SetDocument(doc)
		bw8.writes = append(bw8.writes, mongo.ClientBulkWrite{
			Database: bw8.tx.dbname, Collection: table, Model: m,
		})
	}
}

// Delete 添加删除操作
func (bw8 *BulkWrite8) Delete(model any, where ...any) {
	table, sch, err := bw8.resolve(model)
	if err != nil {
		bw8.Error = err
		return
	}
	query := clause.New()
	query.Where(where[0], where[1:]...)
	filter := query.Build(sch)
	if clause.Multiple(filter) {
		m := mongo.NewClientDeleteManyModel().SetFilter(filter)
		bw8.writes = append(bw8.writes, mongo.ClientBulkWrite{
			Database: bw8.tx.dbname, Collection: table, Model: m,
		})
	} else {
		m := mongo.NewClientDeleteOneModel().SetFilter(filter)
		bw8.writes = append(bw8.writes, mongo.ClientBulkWrite{
			Database: bw8.tx.dbname, Collection: table, Model: m,
		})
	}
}

// Submit 提交所有跨集合操作，原子执行
func (bw8 *BulkWrite8) Submit() error {
	if bw8.Error != nil {
		return bw8.Error
	}
	if len(bw8.writes) == 0 {
		return nil
	}
	if len(bw8.opts) == 0 {
		bw8.opts = append(bw8.opts, options.ClientBulkWrite().SetOrdered(false))
	}
	bw8.tx = bw8.tx.callbacks.Call(bw8.tx, func(db *DB, client *mongo.Client) error {
		var err error
		if bw8.result, err = client.BulkWrite(bw8.ctx, bw8.writes, bw8.opts...); err == nil {
			bw8.writes = nil
		}
		return err
	})
	return bw8.tx.Error
}

// Result 获取上一次 Submit 的结果
func (bw8 *BulkWrite8) Result() *mongo.ClientBulkWriteResult {
	return bw8.result
}

// Options 设置 BulkWrite 选项
func (bw8 *BulkWrite8) Options(opts ...options.Lister[options.ClientBulkWriteOptions]) {
	bw8.opts = append(bw8.opts, opts...)
}

// String 返回待提交操作的 JSON 描述
func (bw8 *BulkWrite8) String() string {
	var logs []bulkWriteLog
	for _, w := range bw8.writes {
		entry := bulkWriteLog{}
		switch m := w.Model.(type) {
		case *mongo.ClientUpdateOneModel:
			entry = bulkWriteLog{Model: "Update", Filter: m.Filter, Value: m.Update}
		case *mongo.ClientUpdateManyModel:
			entry = bulkWriteLog{Model: "UpdateMany", Filter: m.Filter, Value: m.Update}
		case *mongo.ClientInsertOneModel:
			entry = bulkWriteLog{Model: "Insert", Value: m.Document}
		case *mongo.ClientDeleteOneModel:
			entry = bulkWriteLog{Model: "Delete", Filter: m.Filter}
		case *mongo.ClientDeleteManyModel:
			entry = bulkWriteLog{Model: "DeleteMany", Filter: m.Filter}
		}
		logs = append(logs, entry)
	}
	opts := map[string]any{
		"Database":  bw8.tx.dbname,
		"Operation": logs,
	}
	b, _ := json.Marshal(opts)
	return string(b)
}
