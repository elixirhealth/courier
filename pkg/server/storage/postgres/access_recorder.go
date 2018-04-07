package postgres

import (
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"go.uber.org/zap"
)

type accessRecorder struct {
	params  *storage.Parameters
	db      *sql.DB
	dbCache sq.DBProxyContext
	qr      bstorage.Querier
	logger  *zap.Logger
}
