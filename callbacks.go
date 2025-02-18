package cosmo

func initializeCallbacks() *callbacks {
	cb := &callbacks{processors: make(map[string]*processor)}
	cb.processors["query"] = &processor{handle: cmdQuery}
	cb.processors["create"] = &processor{handle: cmdCreate}
	cb.processors["update"] = &processor{handle: cmdUpdate}
	cb.processors["delete"] = &processor{handle: cmdDelete}
	return cb
}

// callbacks gorm callbacks manager
type callbacks struct {
	processors map[string]*processor
}

type processor struct {
	handle executeHandle
}

// Call 自定义调用
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
	if err := p.handle(tx); err != nil {
		tx.Errorf(err)
		return
	}
	//fmt.Printf("Execute:%v,%+v\n", stmt.reflectValue.Kind(), stmt.reflectValue.Interface())
	return
}
