package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"

	sq "github.com/Masterminds/squirrel"
	"github.com/drausin/libri/libri/common/errors"
	stg "github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"go.uber.org/zap"
)

const (
	documentTable = "document"

	valueCol = "value"
)

var (
	fqDocumentTable = cacheSchema + "." + documentTable
)

type cache struct {
	params  *stg.Parameters
	db      *sql.DB
	dbCache sq.DBProxyContext
	qr      bstorage.Querier
	ar      stg.AccessRecorder
	logger  *zap.Logger
}

// New creates a new Cache backed by a Postgres DB with the given db URL.
func New(
	dbURL string, params *stg.Parameters, logger *zap.Logger,
) (stg.Cache, error) {
	if dbURL == "" {
		return nil, errEmptyDBUrl
	}
	if params.Type != bstorage.Postgres {
		return nil, errUnexpectedStorageType
	}
	db, err := sql.Open("postgres", dbURL)
	errors.MaybePanic(err)
	dbCache := sq.NewStmtCacher(db)
	qr := bstorage.NewQuerier()
	ar := &accessRecorder{
		params:  params,
		db:      db,
		dbCache: dbCache,
		qr:      qr,
		logger:  logger,
	}
	return &cache{
		params:  params,
		db:      db,
		dbCache: dbCache,
		qr:      qr,
		ar:      ar,
		logger:  logger,
	}, nil

}

// Put stores the marshaled document value at the hex of its Key.
func (c *cache) Put(key []byte, value []byte) error {
	if len(key) != stg.KeySize {
		return stg.ErrInvalidKeySize
	}
	if len(value) > stg.MaxValueSize {
		return stg.ErrValueTooLarge
	}
	q1 := psql.RunWith(c.dbCache).
		Select(count).
		From(fqDocumentTable).
		Where(sq.Eq{keyCol: key})
	c.logger.Debug("checking in cache", logCacheGet(q1, key)...)
	ctx, cancel := context.WithTimeout(context.Background(), c.params.GetTimeout)
	row := c.qr.SelectQueryRowContext(ctx, q1)
	cancel()
	var exists int
	if err := row.Scan(&exists); err != nil {
		return err
	}
	hexKey := hex.EncodeToString(key)
	if exists == 1 {
		c.logger.Debug("cache already contains value", zap.String(logKey, hexKey))
		return nil
	}
	q2 := psql.RunWith(c.dbCache).
		Insert(fqDocumentTable).
		SetMap(map[string]interface{}{
			keyCol:   key,
			valueCol: value,
		})
	c.logger.Debug("putting into cache", logKeyValue(key, value)...)
	ctx, cancel = context.WithTimeout(context.Background(), c.params.PutTimeout)
	defer cancel()
	if _, err := c.qr.InsertExecContext(ctx, q2); err != nil {
		return err
	}
	c.logger.Debug("put into cache", logKeyValue(key, value)...)
	return nil
}

// Get retrieves the marshaled document value of the given hex Key.
func (c *cache) Get(key []byte) ([]byte, error) {
	q := psql.RunWith(c.dbCache).
		Select(valueCol).
		From(fqDocumentTable).
		Where(sq.Eq{keyCol: key})
	c.logger.Debug("getting from cache", logCacheGet(q, key)...)
	ctx, cancel := context.WithTimeout(context.Background(), c.params.GetTimeout)
	row := c.qr.SelectQueryRowContext(ctx, q)
	cancel()
	var value []byte
	if err := row.Scan(&value); err != nil {
		return nil, err
	}
	c.logger.Debug("got from cache", logKeyValue(key, value)...)
	return value, nil
}

// EvictNext removes the next batch of documents eligible for eviction from the cache.
func (c *cache) EvictNext() error {
	c.logger.Debug("beginning next eviction")
	keys, err := c.ar.GetNextEvictions()
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		c.logger.Debug("evicted no documents")
		return nil
	}
	if err = c.ar.CacheEvict(keys); err != nil {
		return err
	}
	q := psql.RunWith(c.dbCache).
		Delete(fqDocumentTable).
		Where(sq.Eq{keyCol: keys})
	c.logger.Debug("evicting from cache", logCacheEvict(q, keys)...)
	ctx, cancel := context.WithTimeout(context.Background(), c.params.DeleteTimeout)
	defer cancel()
	result, err := c.qr.DeleteExecContext(ctx, q)
	if err != nil {
		return err
	}
	nDeleted, err := result.RowsAffected()
	errors.MaybePanic(err) // should never happen w/ Postgres
	c.logger.Debug("evicted documents", zap.Int(logNDeleted, int(nDeleted)))
	return nil
}
