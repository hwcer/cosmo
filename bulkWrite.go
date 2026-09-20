package cosmo

import (
	"encoding/json"
	"errors"

	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"github.com/hwcer/logger"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type BulkWrite struct {
	tx     *DB
	opts   []options.Lister[options.BulkWriteOptions]
	models []mongo.WriteModel
	result *mongo.BulkWriteResult
	filter BulkWriteUpdateFilter
}

type ModelBulkWriteFilter interface {
	BulkWriteFilter(up update.Update)
}

type BulkWriteUpdateFilter func(up update.Update)

func (this *BulkWrite) SetUpdateFilter(filter BulkWriteUpdateFilter) {
	this.filter = filter
}

// Size 等待提交的事务数量
func (this *BulkWrite) Size() int {
	return len(this.models)
}

// Submit 提交修改。成功后 models 被清空，重复调用返回 nil（无操作）。
//
// 🔴 部分成功只保留失败条目:unordered 批量(默认)返回逐条错误(BulkWriteException)时,
// 按 Index 剔除已生效的 model,只保留失败的那几条等下次 Submit 重试;
// ordered 批量(failure 之后的条目未执行,Index 不可信)与整批级错误(网络断连、
// 写关注失败等无法定位具体条目)全量保留。
// 逐条失败都是确定性错误(重复主键、文档校验等),重发也不会成功 —— 不剔除的话,
// 重试时已生效条目会再次执行(插入类撞重复主键),失败条目则永远卡死队列,连带后续提交全部失败。
func (this *BulkWrite) Submit() (err error) {
	if this.tx.Error != nil {
		return this.tx.Error
	}
	if len(this.models) == 0 {
		return nil
	}
	if len(this.opts) == 0 {
		this.opts = append(this.opts, options.BulkWrite().SetOrdered(false))
	}
	unordered := !this.resolveOrdered()

	this.tx = this.tx.callbacks.Call(this.tx, func(db *DB, client *mongo.Client) error {
		coll := client.Database(db.dbname).Collection(db.stmt.table)
		this.result, err = coll.BulkWrite(db.stmt.Context, this.models, this.opts...)
		if err == nil {
			this.models = nil
		} else if unordered {
			this.retainFailures(err)
		}
		return err
	})
	return this.tx.Err()
}

// retainFailures 部分成功清理:按 WriteErrors.Index 剔除已生效的 model,只保留失败的。
// 重复主键(E11000)常见于「上次整批错误(如断连)后重发」的插入 —— 上次实际已写入,这里一并剔除收敛队列
func (this *BulkWrite) retainFailures(err error) {
	var ex mongo.BulkWriteException
	if !errors.As(err, &ex) || len(ex.WriteErrors) == 0 {
		return //无法定位具体条目:断连/仅写关注失败,全量保留等待重试
	}
	failed := make(map[int]struct{}, len(ex.WriteErrors))
	var table string
	if this.tx != nil && this.tx.stmt != nil {
		table = this.tx.stmt.table
	}
	for _, we := range ex.WriteErrors {
		failed[we.Index] = struct{}{}
		logger.Alert("bulkWrite 部分失败,table:%v,index:%v,code:%v,error:%v", table, we.Index, we.Code, we.Message)
	}
	remain := make([]mongo.WriteModel, 0, len(this.models)-len(failed))
	for i, m := range this.models {
		if _, bad := failed[i]; bad {
			continue
		}
		remain = append(remain, m)
	}
	this.models = remain
}

// resolveOrdered 还原当前 opts 解析后的 Ordered 设置(未设置时为 mongo 默认 true)
func (this *BulkWrite) resolveOrdered() bool {
	opts := &options.BulkWriteOptions{}
	for _, l := range this.opts {
		for _, f := range l.List() {
			_ = f(opts)
		}
	}
	if opts.Ordered == nil {
		return options.DefaultOrdered
	}
	return *opts.Ordered
}
func (this *BulkWrite) update(data any, where []any, includeZeroValue bool) {
	stmt := this.tx.stmt
	if len(where) == 0 {
		_ = this.tx.Errorf(ErrMissingWhereClause)
		return
	}
	query := clause.New()
	query.Where(where[0], where[1:]...)
	//🔴 解析失败/空过滤器不得进入批量提交:空 filter 的 UpdateOne 会静默更新任意
	//一条文档。单条路径(cmdUpdate)有 ErrMissingWhereClause 拦截,批量路径同样必须拦截
	filter := query.Build(stmt.schema)
	if qerr := query.Error(); qerr != nil {
		_ = this.tx.Errorf(qerr)
		return
	}
	if len(filter) == 0 {
		_ = this.tx.Errorf(ErrMissingWhereClause)
		return
	}
	value, upsert, err := update.Build(data, stmt.GetSchema(), stmt.GetSelector(), includeZeroValue)
	if err != nil {
		_ = this.tx.Errorf(err)
		return
	}
	if this.filter != nil {
		this.filter(value)
	}
	model := mongo.NewUpdateOneModel()
	model.SetFilter(filter)
	model.SetUpdate(value)
	if upsert || stmt.upsert {
		model.SetUpsert(true)
	}
	this.models = append(this.models, model)
}

func (this *BulkWrite) Save(data any, where ...any) {
	this.update(data, where, true)
}

// Update 更新
// data   map[string]any  update.Update  bson.M
func (this *BulkWrite) Update(data any, where ...any) {
	this.update(data, where, false)
}

func (this *BulkWrite) Insert(documents ...any) {
	for _, doc := range documents {
		model := mongo.NewInsertOneModel()
		model.SetDocument(doc)
		this.models = append(this.models, model)
	}
}

func (this *BulkWrite) Delete(where ...any) {
	if len(where) == 0 {
		_ = this.tx.Errorf(ErrMissingWhereClause)
		return
	}
	query := clause.New()
	query.Where(where[0], where[1:]...)
	//🔴 同 update():坏条件退化为空 filter 的 DeleteMany 是全集合删除,必须拦截
	filter := query.Build(this.tx.stmt.schema)
	if qerr := query.Error(); qerr != nil {
		_ = this.tx.Errorf(qerr)
		return
	}
	if len(filter) == 0 {
		_ = this.tx.Errorf(ErrMissingWhereClause)
		return
	}
	multiple := clause.Multiple(filter)

	if multiple {
		model := mongo.NewDeleteManyModel()
		model.SetFilter(filter)
		this.models = append(this.models, model)
	} else {
		model := mongo.NewDeleteOneModel()
		model.SetFilter(filter)
		this.models = append(this.models, model)
	}
}

func (this *BulkWrite) Result() *mongo.BulkWriteResult {
	return this.result
}

func (this *BulkWrite) Options(opts ...options.Lister[options.BulkWriteOptions]) {
	this.opts = append(this.opts, opts...)
}

type bulkWriteLog struct {
	Model  string
	Filter any
	Value  any
}

func (this *BulkWrite) String() string {
	var logs []bulkWriteLog
	for _, i := range this.models {
		switch model := i.(type) {
		case *mongo.UpdateOneModel:
			logs = append(logs, bulkWriteLog{Model: "Update", Filter: model.Filter, Value: model.Update})
		case *mongo.InsertOneModel:
			logs = append(logs, bulkWriteLog{Model: "Insert", Value: model.Document})
		case *mongo.DeleteOneModel:
			logs = append(logs, bulkWriteLog{Model: "Delete", Filter: model.Filter})
		case *mongo.DeleteManyModel:
			logs = append(logs, bulkWriteLog{Model: "Delete", Filter: model.Filter})
		}
	}

	opts := map[string]any{}
	opts["Database"] = this.tx.dbname
	opts["Collection"] = this.tx.stmt.table
	opts["Operation"] = logs

	b, _ := json.Marshal(opts)

	return string(b)
}
