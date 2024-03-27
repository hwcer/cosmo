package cosmo

import (
	"context"
	"fmt"
	"github.com/hwcer/schema"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)

// AutoMigrator returns migrator
// Sparse
func (db *DB) AutoMigrator(dst ...interface{}) error {
	for _, mod := range dst {
		sch, err := schema.Parse(mod)
		if err != nil {
			return err
		}
		indexes := sch.ParseIndexes()
		for _, index := range indexes {
			if e := db.indexes(mod, index); e != nil {
				return fmt.Errorf("AutoMigrator[%v.%v]:%v", db.dbname, sch.Table, e)
			}
		}
	}
	return nil
}

func (db *DB) indexes(model interface{}, index *schema.Index) (err error) {
	tx, coll := db.Collection(model)
	if tx.Error != nil {
		return tx.Error
	}
	indexView := coll.Indexes()
	_, err = indexView.CreateOne(context.Background(), index.Build())
	if cv, ok := err.(mongo.CommandError); ok && cv.Code == 85 || strings.HasPrefix(cv.Message, "Index already exists with a different name") {
		err = nil
	}
	return
}
