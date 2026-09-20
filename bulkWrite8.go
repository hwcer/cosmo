package cosmo

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosgo/values"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"github.com/hwcer/logger"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// BulkWrite8 跨集合批量写入，需要 MongoDB 8.0+
// 通过 client.BulkWrite() 实现。⚠️ 整批不具备事务原子性(单条原子、整批不回滚):
// 需要跨集合 all-or-nothing 请显式使用多文档事务。失败保留语义见 Submit
type BulkWrite8 struct {
	tx      *DB
	ctx     context.Context
	writes  []mongo.ClientBulkWrite
	opts    []options.Lister[options.ClientBulkWriteOptions]
	result  *mongo.ClientBulkWriteResult
	schemas map[reflect.Type]*bulkWrite8Schema
	Error   *values.Message // 构建阶段的错误, 已由 NormalizeError 统一转换
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
	if len(where) == 0 {
		bw8.Error = NormalizeError(ErrMissingWhereClause)
		return
	}
	table, sch, err := bw8.resolve(model)
	if err != nil {
		bw8.Error = NormalizeError(err)
		return
	}
	query := clause.New()
	query.Where(where[0], where[1:]...)
	//🔴 解析失败/空过滤器不得进入批量提交:空 filter 会静默更新任意一条文档
	filter := query.Build(sch)
	if qerr := query.Error(); qerr != nil {
		bw8.Error = NormalizeError(qerr)
		return
	}
	if len(filter) == 0 {
		bw8.Error = NormalizeError(ErrMissingWhereClause)
		return
	}
	value, upsert, err := update.Build(data, sch, nil, includeZeroValue)
	if err != nil {
		bw8.Error = NormalizeError(err)
		return
	}
	if f, ok := model.(ModelBulkWriteFilter); ok {
		f.BulkWriteFilter(value)
	}
	m := mongo.NewClientUpdateOneModel().SetFilter(filter).SetUpdate(value)
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

// Insert 添加插入操作
func (bw8 *BulkWrite8) Insert(model any, documents ...any) {
	table, _, err := bw8.resolve(model)
	if err != nil {
		bw8.Error = NormalizeError(err)
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
	if len(where) == 0 {
		bw8.Error = NormalizeError(ErrMissingWhereClause)
		return
	}
	table, sch, err := bw8.resolve(model)
	if err != nil {
		bw8.Error = NormalizeError(err)
		return
	}
	query := clause.New()
	query.Where(where[0], where[1:]...)
	//🔴 同 update():坏条件退化为空 filter 的 DeleteMany 是全集合删除,必须拦截
	filter := query.Build(sch)
	if qerr := query.Error(); qerr != nil {
		bw8.Error = NormalizeError(qerr)
		return
	}
	if len(filter) == 0 {
		bw8.Error = NormalizeError(ErrMissingWhereClause)
		return
	}
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

// Submit 提交所有跨集合批量写入。
//
// 🔴 MongoDB 8.0 bulkWrite 整批**不具备事务原子性**(单条原子、整批不回滚,
// 除非显式包多文档事务),注释与文档不得再宣称"原子提交"。
// 部分成功语义与单集合版 BulkWrite 对齐:unordered(本结构默认)返回逐条错误时
// 按 Index 剔除已生效条目只保留失败的等下次重试;ordered(failure 之后的条目
// 未执行,Index 不可信)与整批级错误全量保留。
// 不剔除的话重发时 $inc/$push 类已生效条目被重复应用,跨集合版本连
// "失败卡死"提示都没有,是静默双写。
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
	unordered := !bw8.resolveOrdered()
	err := bw8.tx.pool.Execute(bw8.ctx, func(client *mongo.Client) (err error) {
		if bw8.result, err = client.BulkWrite(bw8.ctx, bw8.writes, bw8.opts...); err == nil {
			bw8.writes = nil
		} else if unordered {
			bw8.retainFailures(err)
		}
		return
	})
	if err != nil {
		return NormalizeError(err)
	}
	return nil
}

// resolveOrdered 还原当前 opts 解析后的 Ordered 设置
// 🔴 未显式设置 Ordered 的自定义 opts,驱动按 ordered 执行(v2 ClientBulkWrite 默认
// ordered=true);此处若按 unordered 剔除,ordered 下失败之前已生效的条目会被保留重发,
// $inc/$push 类重复应用——恰是 retainFailures 要防的事。nil 一律兜底到 ordered(全量保留,保守侧)
func (bw8 *BulkWrite8) resolveOrdered() bool {
	opts := &options.ClientBulkWriteOptions{}
	for _, l := range bw8.opts {
		for _, f := range l.List() {
			_ = f(opts)
		}
	}
	if opts.Ordered == nil {
		return true
	}
	return *opts.Ordered
}

// retainFailures 部分成功清理:按 ClientBulkWriteException.WriteErrors 的 Index
// 剔除已生效条目,只保留失败的。WriteErrors 为空(断连/写关注失败等无法定位
// 具体条目)时全量保留等待重试
func (bw8 *BulkWrite8) retainFailures(err error) {
	var ex mongo.ClientBulkWriteException
	if !errors.As(err, &ex) || len(ex.WriteErrors) == 0 {
		return
	}
	remain := make([]mongo.ClientBulkWrite, 0, len(bw8.writes))
	for i, w := range bw8.writes {
		if we, bad := ex.WriteErrors[i]; bad {
			logger.Alert("bulkWrite8 部分失败,collection:%v,index:%v,code:%v,error:%v", w.Collection, i, we.Code, we.Message)
			continue
		}
		remain = append(remain, w)
	}
	bw8.writes = remain
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
