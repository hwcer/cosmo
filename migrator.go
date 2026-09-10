package cosmo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hwcer/cosgo/schema"
	"github.com/hwcer/cosmo/clause"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// AutoMigrator 自动迁移功能，根据模型定义自动创建或更新索引
// dst: 要迁移的模型对象，可以传入多个模型
// 返回值: 迁移过程中发生的错误
func (db *DB) AutoMigrator(dst ...any) error {
	for _, mod := range dst {
		// 解析模型获取元数据
		sch, err := schema.Parse(mod)
		if err != nil {
			return err
		}
		// 解析模型定义的索引
		indexes := sch.ParseIndexes()
		for _, index := range indexes {
			// 创建或更新索引
			if e := db.indexes(mod, index); e != nil {
				return fmt.Errorf("AutoMigrator[%v.%v]:%v", db.dbname, sch.Table, e)
			}
		}
	}
	return nil
}

func (db *DB) indexes(model any, index *schema.Index) (err error) {
	tx, coll := db.Collection(model)
	if tx.Error != nil {
		return tx.Error
	}
	indexView := coll.Indexes()
	mongoIndex, err := index.Build(db.indexPartialBuild)
	if err != nil {
		return err
	}
	_, err = indexView.CreateOne(context.Background(), *mongoIndex)
	var cv mongo.CommandError
	if errors.As(err, &cv) && cv.Code == CodeIndexOptionsConflict || strings.HasPrefix(cv.Message, "Index already exists with a different name") {
		err = nil
	}
	return
}

func (db *DB) indexPartialBuild(sch *schema.Schema, where []string) (any, error) {
	//必须用clause.New():字面量构造的Query其complex为nil,
	//含OR的where条件会向nil map写入而panic
	q := clause.New()
	for _, v := range where {
		q.Where(v)
	}
	r := q.Build(sch)
	return r, nil
}
