package cosmo

import (
	"context"
	"github.com/hwcer/cosmo/clause"
	update2 "github.com/hwcer/cosmo/update"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BulkWrite struct {
	tx     *DB
	opts   []*options.BulkWriteOptions
	models []mongo.WriteModel
}

func (this *BulkWrite) Save() (result *mongo.BulkWriteResult, err error) {
	if len(this.models) == 0 {
		return nil, nil
	}
	tx := this.tx.callbacks.Call(this.tx, func(db *DB) error {
		coll := db.client.Database(db.dbname).Collection(db.Statement.Table)
		if result, err = coll.BulkWrite(context.Background(), this.models, this.opts...); err == nil {
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
	update, _ := update2.Build(data, Schema, nil)

	model := mongo.NewUpdateOneModel()
	model.SetFilter(query.Build(this.tx.Statement.Schema))
	model.SetUpdate(update)
	if _, ok := update[update2.UpdateTypeSetOnInsert]; ok {
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
	filter := query.Build(this.tx.Statement.Schema)
	multiple := clause.Multiple(filter)

	if multiple {
		model := mongo.NewDeleteManyModel()
		model.SetFilter(query)
		this.models = append(this.models, model)
	} else {
		model := mongo.NewDeleteOneModel()
		model.SetFilter(query)
		this.models = append(this.models, model)
	}
}
