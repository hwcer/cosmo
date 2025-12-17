package cosmo

import (
	"reflect"

	"go.mongodb.org/mongo-driver/mongo"
)

// initializeCallbacks 初始化回调管理器
// 创建默认的处理器映射，包括查询、创建、更新和删除操作
func initializeCallbacks() *callbacks {
	cb := &callbacks{processors: make(map[string]*processor)}
	cb.processors["query"] = &processor{handle: cmdQuery}   // 查询操作处理器
	cb.processors["create"] = &processor{handle: cmdCreate} // 创建操作处理器
	cb.processors["update"] = &processor{handle: cmdUpdate} // 更新操作处理器
	cb.processors["delete"] = &processor{handle: cmdDelete} // 删除操作处理器
	return cb
}

// callbacks 回调管理器，用于管理不同类型的数据库操作处理器
type callbacks struct {
	processors map[string]*processor // 处理器映射，键为操作类型，值为对应的处理器
}

// processor 操作处理器，用于执行具体的数据库操作
type processor struct {
	handle executeHandle // 操作处理函数
}

// Call 执行自定义调用
// db: 数据库连接实例
// handle: 自定义处理函数
// 返回值: 执行结果的数据库连接实例
func (cs *callbacks) Call(db *DB, handle executeHandle) *DB {
	p := &processor{handle: handle}
	return p.Execute(db)
}

func (cs *callbacks) Create() *processor {
	return cs.processors["create"]
}

func (cs *callbacks) Query() *processor {
	return cs.processors["query"]
}

func (cs *callbacks) Update() *processor {
	return cs.processors["update"]
}

func (cs *callbacks) Delete() *processor {
	return cs.processors["delete"]
}

// Execute 执行操作
//
//	handle func(tx *DB,query BuildUpdate.M) error
func (p *processor) Execute(db *DB) (tx *DB) {
	tx = db.stmt.Parse()
	if tx.Error != nil {
		return
	}

	stmt := tx.stmt
	if stmt.table == "" {
		tx.Errorf("table not set, please set it like: db.model(&user) or db.table(\"users\") %+v")
	}
	//value || model 类型为Struct并且主键不为空时，设置为查询条件
	//var reflectModel reflect.Value
	//if stmt.model != nil {
	//	reflectModel = reflect.Indirect(reflect.ValueOf(stmt.model))
	//} else if stmt.reflectValue.Kind() == reflect.Struct {
	//	reflectModel = stmt.reflectValue
	//}
	//if reflectModel.IsValid() && !reflectModel.IsZero() {
	//	field := stmt.schema.LookUpField(clause.MongoPrimaryName)
	//	if field != nil {
	//		v := reflectModel.FieldByIndex(field.StructField.Index)
	//		if v.IsValid() && !v.IsZero() {
	//			stmt.Clause.Primary(v.Interface())
	//		}
	//	}
	//}

	if p.handle == nil || tx.Error != nil {
		return
	}
	//defer tx.reset()
	// 使用PoolManager.Execute获取client并传递给handle
	err := tx.pool.Execute(stmt.Context, func(client *mongo.Client) error {
		return p.handle(tx, client)
	})
	if err != nil {
		tx.Errorf(err)
		return
	}
	//清理val
	stmt.value = nil
	stmt.reflectValue = reflect.Value{}

	//fmt.Printf("Execute:%v,%+v\n", stmt.reflectValue.Kind(), stmt.reflectValue.Interface())
	return
}
