package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"

	sq "github.com/Masterminds/squirrel"
	"github.com/drausin/libri/libri/common/errors"
	"github.com/elixirhealth/courier/pkg/server/storage"
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
	params  *storage.Parameters
	db      *sql.DB
	dbCache sq.DBProxyContext
	qr      bstorage.Querier
	ar      storage.AccessRecorder
	logger  *zap.Logger
}

// New creates a new Cache backed by a Postgres DB with the given db URL.
func New(
	dbURL string, params *storage.Parameters, logger *zap.Logger,
) (storage.Cache, storage.AccessRecorder, error) {
	if dbURL == "" {
		return nil, nil, errEmptyDBUrl
	}
	if params.Type != bstorage.Postgres {
		return nil, nil, errUnexpectedStorageType
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
	}, ar, nil

}

// Put stores the marshaled document value at the hex of its Key
func (c *cache) Put(key []byte, value []byte) (bool, error) {
	if len(key) != storage.KeySize {
		return false, storage.ErrInvalidKeySize
	}
	if len(value) > storage.MaxValueSize {
		return false, storage.ErrValueTooLarge
	}
	q := psql.RunWith(c.dbCache).
		Insert(fqDocumentTable).
		SetMap(map[string]interface{}{
			keyCol:   key,
			valueCol: value,
		}).
		Suffix(onConflictDoNothing)
	c.logger.Debug("putting into cache", logKeyValue(key, value)...)
	ctx, cancel := context.WithTimeout(context.Background(), c.params.PutTimeout)
	result, err := c.qr.InsertExecContext(ctx, q)
	cancel()
	if err != nil {
		return false, err
	}
	nInserted, err := result.RowsAffected()
	errors.MaybePanic(err) // should never happen w/ Postgres
	if nInserted == 0 {
		hexKey := hex.EncodeToString(key)
		c.logger.Debug("cache already contains value", zap.String(logKey, hexKey))
		return true, nil
	}
	if err = c.ar.CachePut(key); err != nil {
		return false, err
	}
	c.logger.Debug("put into cache", logKeyValue(key, value)...)
	return false, nil
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
	defer cancel()
	var value []byte
	if err := row.Scan(&value); err != nil && err != sql.ErrNoRows {
		return nil, err
	} else if err == sql.ErrNoRows {
		return nil, storage.ErrMissingValue
	}
	if err := c.ar.CacheGet(key); err != nil {
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

// Close cleans up an resources held by the cache.
func (c *cache) Close() error {
	return c.db.Close()
}
