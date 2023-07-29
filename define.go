package cosmo

const DBNameUpdate = "update"

// Plugin GORM plugin interface
//type Plugin interface {
//	Name() string
//	Initialize(*DB) error
//}

type executeHandle func(db *DB) error
