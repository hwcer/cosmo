package cosmo

import "go.mongodb.org/mongo-driver/mongo"

const FieldNameUpdate = "update" //UPDATE

type executeDone func(db *DB) error

type executeHandle func(db *DB, client *mongo.Client) error

type Cursor interface {
	Decode(val interface{}) error
}
