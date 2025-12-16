package cosmo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// DB GORM DB definition
type DB struct {
	*Config
	stmt         *Statement
	clone        bool //是否克隆体
	Error        error
	RowsAffected int64 //操作影响的条数
}

// New
// address uri || *mongo.Client
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

func (db *DB) Start(dbname string, address interface{}) (err error) {
	db.dbname = dbname
	var uri string
	switch address.(type) {
	case string:
		uri = address.(string)
		// 当传入连接字符串时，使用NewPoolManager创建客户端
		db.Config.pool = NewPoolManager(uri, DefaultPoolConfig())
		db.Config.pool.Start()
	case *PoolManager:
		db.Config.pool = address.(*PoolManager)
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

func (db *DB) Close() (err error) {
	if db.pool != nil {
		// 通过poolManager关闭client
		if err = db.pool.client.Disconnect(context.Background()); err != nil {
			return
		}
		// 这里可以添加PoolManager的关闭逻辑，如果有的话
	}
	return
}

// Session create new db session
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

// Database 新数据库
func (db *DB) Database(dbname string) *DB {
	return db.Session(&Session{DBName: dbname})
}
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

// BulkWrite 批量写入
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

// WithContext change current instance db's context to ctx
func (db *DB) WithContext(ctx context.Context) *DB {
	return db.Session(&Session{Context: ctx})
}

// Errorf add error to db
func (db *DB) Errorf(format interface{}, args ...interface{}) *DB {
	switch v := format.(type) {
	case string:
		db.Error = fmt.Errorf(v, args...)
	default:
		db.Error = fmt.Errorf("%v", format)
	}
	return db
}

func (db *DB) getInstance() *DB {
	if db.clone {
		return db
	}
	tx := &DB{Config: db.Config, clone: true}
	tx.stmt = NewStatement(tx)
	return tx
}

//func (db *DB) Use(plugin Plugin) error {
//	name := plugin.Name()
//	if _, ok := db.Plugins[name]; ok {
//		return ErrRegistered
//	}
//	if err := plugin.Initialize(db); err != nil {
//		return err
//	}
//	db.Plugins[name] = plugin
//	return nil
//}

func (db *DB) ObjectID() primitive.ObjectID {
	return primitive.NewObjectID()
}
