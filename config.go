package cosmo

// Config GORM config
type Config struct {
	pool      *PoolManager
	models    []any
	dbname    string
	callbacks *callbacks
}

// Register 预注册的MODEL在启动时会自动创建索引
func (c *Config) Register(model interface{}) {
	c.models = append(c.models, model)
}
