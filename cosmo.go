// Package cosmo 是一个轻量级的 MongoDB ORM 框架，提供了类似 GORM 的接口，支持模型映射、查询构建和批量操作。
// 它包含连接池管理、事务支持、自动迁移、缓存机制等功能，适用于构建高性能的 MongoDB 应用程序。
package cosmo

import (
	"context"
	"errors"
	"fmt"

	"github.com/hwcer/cosmo/health"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// DB 是 Cosmo ORM 框架的核心结构体，提供了数据库操作的入口点。
// 它封装了数据库连接、事务管理、模型映射等功能，支持链式操作。
type DB struct {
	*Config                 // 数据库配置
	stmt         *Statement // 数据库操作语句
	clone        bool       // 是否为克隆体
	Error        error      // 错误信息
	RowsAffected int64      // 操作影响的条数
}

// New 创建一个新的 Cosmo DB 实例。
// 参数 configs 是可选的数据库配置，可以设置连接池、日志、插件等选项。
// 如果不提供配置，将使用默认配置。
// 返回值是 DB 实例，作为所有数据库操作的入口点。
//
// 使用示例：
//
//	db := cosmo.New(&cosmo.Config{
//	    PoolConfig: &cosmo.PoolConfig{
//	        Address: "mongodb://localhost:27017",
//	        CheckInterval: 30 * time.Second,
//	    },
//	})
func New(configs ...*Config) (db *DB) {
	var config *Config
	if len(configs) > 0 {
		config = configs[0]
	} else {
		config = &Config{}
	}

	//if config.Plugins == nil {
	//	config.Plugins = map[string]Plugin{}
	//}
	db = &DB{Config: config}
	db.callbacks = initializeCallbacks()
	db.stmt = NewStatement(db)
	return
}

// Start 初始化数据库连接并启动连接池。
// 参数 dbname 是要使用的数据库名称。
// 参数 address 可以是 MongoDB 连接字符串或 *PoolManager 实例。
// 返回值是可能的错误信息。
//
// 使用示例：
// err := db.Start("mydatabase", "mongodb://localhost:27017")
// 或
// pool := cosmo.NewPoolManager("mongodb://localhost:27017")
// err := db.Start("mydatabase", pool)
func (db *DB) Start(dbname string, address interface{}) (err error) {
	db.dbname = dbname
	var uri string
	switch address.(type) {
	case string:
		uri = address.(string)
		db.Config.pool = health.New(uri)
		db.Config.pool.Start()
	case *health.Manager:
		db.Config.pool = address.(*health.Manager)
		db.Config.pool.Start()
	default:
		err = errors.New("address error")
		return
	}

	if err = db.AutoMigrator(db.models...); err != nil {
		return
	}
	return
}

// Session 创建一个新的数据库会话。
// 参数 session 包含会话配置，如数据库名称、上下文等。
// 返回值是新的 DB 实例，用于执行会话相关的操作。
//
// 使用示例：
//
//	sessionDB := db.Session(&cosmo.Session{
//	    DBName: "newdatabase",
//	    Context: ctx,
//	})
func (db *DB) Session(session *Session) *DB {
	var (
		config = *db.Config
		tx     = &DB{
			Config: &config,
			Error:  db.Error,
			clone:  false,
		}
	)

	tx.stmt = NewStatement(tx)

	if session.DBName != "" {
		tx.Config.dbname = session.DBName
	}

	if session.Context != nil {
		tx.stmt.Context = session.Context
	}

	//if session.Logger != nil {
	//	tx.Config.Logger = config.Logger
	//}

	return tx
}

// Database 创建一个指向指定数据库的新 DB 实例。
// 参数 dbname 是要使用的新数据库名称。
// 返回值是新的 DB 实例，用于操作指定的数据库。
//
// 使用示例：
// newDB := db.Database("newdatabase")
func (db *DB) Database(dbname string) *DB {
	return db.Session(&Session{DBName: dbname})
}

// Collection 获取指定模型或集合名称对应的 MongoDB 集合。
// 参数 model 可以是结构体类型或集合名称字符串。
// 返回值 tx 是新的 DB 实例，coll 是 MongoDB 集合对象。
//
// 使用示例：
// tx, coll := db.Collection(&User{})
// 或
// tx, coll := db.Collection("users")
func (db *DB) Collection(model any) (tx *DB, coll *mongo.Collection) {
	switch model.(type) {
	case string:
		tx = db.Table(model.(string))
	default:
		tx = db.Model(model)
	}
	tx = tx.callbacks.Call(tx, func(tx *DB, client *mongo.Client) error {
		coll = client.Database(tx.dbname).Collection(tx.stmt.table)
		return nil
	})
	return
}

// BulkWrite 创建批量写入操作实例。
// 参数 model 是要操作的模型类型。
// 参数 filter 是可选的批量更新过滤器。
// 返回值是 BulkWrite 实例，用于构建和执行批量写入操作。
//
// 使用示例：
//
//	bw := db.BulkWrite(&User{}, cosmo.BulkWriteUpdateFilter{
//	    UpdateBy: "_id",
//	})
//
// bw.InsertMany(users)
// bw.UpdateMany(updates)
// result, err := bw.Execute()
func (db *DB) BulkWrite(model any, filter ...BulkWriteUpdateFilter) *BulkWrite {
	tx := db.Model(model)
	tx = tx.stmt.Parse()
	bw := &BulkWrite{tx: tx}
	if len(filter) > 0 {
		bw.SetUpdateFilter(filter[0])
	} else if modelBulkWriteFilter, ok := model.(ModelBulkWriteFilter); ok {
		bw.SetUpdateFilter(modelBulkWriteFilter.BulkWriteFilter)
	}
	return bw
}

// WithContext 为当前数据库实例设置新的上下文。
// 参数 ctx 是要设置的上下文对象。
// 返回值是新的 DB 实例，包含更新后的上下文。
//
// 使用示例：
// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// defer cancel()
// dbWithCtx := db.WithContext(ctx)
func (db *DB) WithContext(ctx context.Context) *DB {
	return db.Session(&Session{Context: ctx})
}

// Errorf 为数据库实例设置格式化错误信息。
// 参数 format 是错误格式化字符串或错误对象。
// 参数 args 是格式化参数。
// 返回值是当前 DB 实例，方便链式调用。
//
// 使用示例：
// db.Errorf("操作失败: %v", err)
func (db *DB) Errorf(format interface{}, args ...interface{}) *DB {
	switch v := format.(type) {
	case string:
		db.Error = fmt.Errorf(v, args...)
	default:
		db.Error = fmt.Errorf("%v", format)
	}
	return db
}

// getInstance 获取数据库实例的克隆体，用于避免并发操作时的状态污染。
// 返回值是新的 DB 实例克隆体。
// 这是一个内部方法，主要用于框架内部实现链式操作。
func (db *DB) getInstance() *DB {
	if db.clone {
		return db
	}
	tx := &DB{Config: db.Config, clone: true}
	tx.stmt = NewStatement(tx)
	return tx
}

func (db *DB) ObjectID() primitive.ObjectID {
	return primitive.NewObjectID()
}
