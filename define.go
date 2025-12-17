package cosmo

import "go.mongodb.org/mongo-driver/mongo"

const FieldNameUpdate = "update" //UPDATE

type executeHandle func(db *DB, client *mongo.Client) error

type Cursor interface {
	Decode(val interface{}) error
}
