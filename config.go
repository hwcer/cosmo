package cosmo

import (
	"go.mongodb.org/mongo-driver/mongo"
)

// Config GORM config
type Config struct {
	models    []any
	dbname    string
	client    *mongo.Client
	callbacks *callbacks
}

//func (c *Config) AfterInitialize(db *DB) error {
//	if db != nil {
//		for _, plugin := range c.Plugins {
//			if err := plugin.Initialize(db); err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}

// Register 预注册的MODEL在启动时会自动创建索引
func (c *Config) Register(model interface{}) {
	c.models = append(c.models, model)
}
