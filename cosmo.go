package cosmo

import (
	"context"
	"errors"

	"github.com/hwcer/cosgo/values"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// DB GORM DB definition
type DB struct {
	*Config
	clone        bool //是否克隆体
	statement    *Statement
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
	db.statement = NewStatement(db)
	return
}

func (db *DB) Start(dbname string, address interface{}) (err error) {
	db.dbname = dbname
	switch address.(type) {
	case string:
		db.Config.client, err = NewClient(address.(string))
	case *mongo.Client:
		db.Config.client = address.(*mongo.Client)
	default:
		err = errors.New("address error")
	}
	if err != nil {
		return
	}
	if err = db.AutoMigrator(db.models...); err != nil {
		return
	}
	return
}

func (db *DB) Close() (err error) {
	if db.client != nil {
		err = db.client.Disconnect(context.Background())
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

	tx.statement = NewStatement(tx)

	if session.DBName != "" {
		tx.Config.dbname = session.DBName
	}

	if session.Context != nil {
		tx.statement.Context = session.Context
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
	tx = tx.callbacks.Call(tx, func(tx *DB) error {
		coll = tx.client.Database(tx.dbname).Collection(tx.statement.table)
		return nil
	})
	return
}

// BulkWrite 批量写入
func (db *DB) BulkWrite(model interface{}) *BulkWrite {
	tx := db.Model(model)
	tx = tx.statement.Parse()
	return &BulkWrite{tx: tx}
}

// WithContext change current instance db's context to ctx
func (db *DB) WithContext(ctx context.Context) *DB {
	return db.Session(&Session{Context: ctx})
}

// Errorf add error to db
func (db *DB) Errorf(format interface{}, args ...interface{}) *DB {
	db.Error = values.Errorf(0, format, args...)
	return db
}

func (db *DB) getInstance() *DB {
	if db.clone {
		return db
	}
	tx := &DB{Config: db.Config, clone: true}
	tx.statement = NewStatement(tx)
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
