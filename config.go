package cosmo

import (
	"github.com/hwcer/cosgo/logger"
	"github.com/hwcer/cosmo/schema"
	"go.mongodb.org/mongo-driver/mongo"
)

// Config GORM config
type Config struct {
	Schema    *schema.Store
	Logger    logger.Interface
	Plugins   map[string]Plugin
	dbname    string
	client    *mongo.Client
	callbacks *callbacks
}

func (c *Config) AfterInitialize(db *DB) error {
	if db != nil {
		for _, plugin := range c.Plugins {
			if err := plugin.Initialize(db); err != nil {
				return err
			}
		}
	}
	return nil
}
