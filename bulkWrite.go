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
	upsert, _ := update.Build(data, this.tx.Statement)

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
