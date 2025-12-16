package cosmo

import "go.mongodb.org/mongo-driver/mongo"

const DBNameUpdate = "update"

// Plugin GORM plugin interface
//type Plugin interface {
//	Name() string
//	Initialize(*DB) error
//}

type executeHandle func(db *DB, client *mongo.Client) error
