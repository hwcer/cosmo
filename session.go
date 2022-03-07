package cosmo

import (
	"context"
	"github.com/hwcer/cosgo/library/logger"
	"time"
)

// Session session config when create session with Session() method
type Session struct {
	DBName string
	//DryRun                   bool
	//PrepareStmt              bool
	NewDB     bool
	SkipHooks bool
	//SkipDefaultTransaction   bool
	//DisableNestedTransaction bool
	//AllowGlobalUpdate        bool
	//FullSaveAssociations     bool
	//QueryFields              bool
	Context context.Context
	Logger  logger.Interface
	NowTime func() time.Time
	//CreateBatchSize          int
}
