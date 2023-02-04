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
}

func (this *BulkWrite) Save() (err error) {
	if this.tx.Statement.Error != nil {
		return this.tx.Statement.Error
	}
	if len(this.models) == 0 {
		return nil
	}
	tx := this.tx.callbacks.Call(this.tx, func(db *DB) error {
		coll := db.client.Database(db.dbname).Collection(db.Statement.Table)
		if this.result, err = coll.BulkWrite(context.Background(), this.models, this.opts...); err == nil {
			this.models = nil
		}
		return err
	})
	err = tx.Error
	return
}

func (this *BulkWrite) Update(data interface{}, where ...interface{}) {
	query := clause.New()
	query.Where(where[0], where[1:]...)
	upsert, err := update.Build(data, this.tx.Statement.schema, &this.tx.Statement.Selector)
	if err != nil {
		_ = this.tx.Errorf(err)
		return
	}
	model := mongo.NewUpdateOneModel()
	model.SetFilter(query.Build(this.tx.Statement.schema))
	model.SetUpdate(upsert)
	if _, ok := upsert[update.UpdateTypeSetOnInsert]; ok {
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
	filter := query.Build(this.tx.Statement.schema)
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
