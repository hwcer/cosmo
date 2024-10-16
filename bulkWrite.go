package cosmo

import (
	"context"
	"github.com/hwcer/cosmo/clause"
	"github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BulkWrite struct {
	tx     *DB
	opts   []*options.BulkWriteOptions
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

func (this *BulkWrite) Save() (err error) {
	if this.tx.statement.Error != nil {
		return this.tx.statement.Error
	}
	if len(this.models) == 0 {
		return nil
	}
	if len(this.opts) == 0 {
		ordered := false
		this.opts = append(this.opts, &options.BulkWriteOptions{Ordered: &ordered})
	}

	tx := this.tx.callbacks.Call(this.tx, func(db *DB) error {
		coll := db.client.Database(db.dbname).Collection(db.statement.table)
		if this.result, err = coll.BulkWrite(context.Background(), this.models, this.opts...); err == nil {
			this.models = nil
		}
		return err
	})
	err = tx.Error
	return
}

// Update 更新
// data   map[string]any  update.Update  bson.M
func (this *BulkWrite) Update(data any, where ...interface{}) {
	stmt := this.tx.statement
	query := clause.New()
	query.Where(where[0], where[1:]...)
	value, upsert, err := update.Build(data, stmt.schema, &stmt.selector)
	if err != nil {
		_ = this.tx.Errorf(err)
		return
	}
	if this.filter != nil {
		this.filter(value)
	}
	model := mongo.NewUpdateOneModel()
	model.SetFilter(query.Build(stmt.schema))
	model.SetUpdate(value)
	if upsert || stmt.upsert {
		model.SetUpsert(true)
	}
	this.models = append(this.models, model)
}

func (this *BulkWrite) Insert(documents ...interface{}) {
	for _, doc := range documents {
		model := mongo.NewInsertOneModel()
		model.SetDocument(doc)
		this.models = append(this.models, model)
	}
}

func (this *BulkWrite) Delete(where ...interface{}) {
	query := clause.New()
	query.Where(where[0], where[1:]...)
	filter := query.Build(this.tx.statement.schema)
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

func (this *BulkWrite) Options(opts ...*options.BulkWriteOptions) {
	this.opts = append(this.opts, opts...)
}
