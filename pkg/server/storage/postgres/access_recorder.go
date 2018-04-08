package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	errors2 "github.com/drausin/libri/libri/common/errors"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	cacheSchema       = "cache"
	accessRecordTable = "access_record"

	keyCol               = "key"
	cachePutOccurredCol  = "cache_put_occurred"
	cachePutTimeMinCol   = "cache_put_time_min"
	cachePutTimeMicroCol = "cache_put_time_micro"
	libriPutOccurredCol  = "libri_put_occurred"
	libriPutTimeMicroCol = "libri_put_time_micro"
	cacheGetOccurredCol  = "cache_get_occurred"
	cacheGetTimeMicroCol = "cache_get_time_micro"

	tblAlias1           = "t1"
	tblAlias2           = "t2"
	onConflictDoNothing = "ON CONFLICT DO NOTHING"
	count               = "COUNT(*)"
	andSpace            = " AND "
)

var (
	psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	fqAccessRecordTable = cacheSchema + "." + accessRecordTable

	errEmptyDBUrl            = errors.New("empty DB URL")
	errUnexpectedStorageType = errors.New("unexpected storage type")
)

type accessType int

const (
	cachePut accessType = iota
	libriPut
	cacheGet
)

func (at accessType) String() string {
	switch at {
	case cachePut:
		return "CACHE_PUT"
	case libriPut:
		return "LIBRI_PUT"
	case cacheGet:
		return "CACHE_GET"
	default:
		panic("should never get here")
	}
}

type accessRecorder struct {
	params  *storage.Parameters
	db      *sql.DB
	dbCache sq.DBProxyContext
	qr      bstorage.Querier
	logger  *zap.Logger
}

func New(
	dbURL string, params *storage.Parameters, logger *zap.Logger,
) (storage.AccessRecorder, error) {
	if dbURL == "" {
		return nil, errEmptyDBUrl
	}
	if params.Type != bstorage.Postgres {
		return nil, errUnexpectedStorageType
	}
	db, err := sql.Open("postgres", dbURL)
	errors2.MaybePanic(err)
	return &accessRecorder{
		params:  params,
		db:      db,
		dbCache: sq.NewStmtCacher(db),
		qr:      bstorage.NewQuerier(),
		logger:  logger,
	}, nil
}

// CachePut creates a new access record with the cache's put time for the document with the given
// Key.
func (ar *accessRecorder) CachePut(key []byte) error {
	return ar.recordAccess(key, cachePut, true)
}

// LibriPut updates the access record's latest libri put time.
func (ar *accessRecorder) LibriPut(key []byte) error {
	return ar.recordAccess(key, libriPut, true)
}

// CacheGet updates the access record's latest get time for the document with the given Key.
func (ar *accessRecorder) CacheGet(key []byte) error {
	return ar.recordAccess(key, cacheGet, false)
}

// CacheEvict deletes the access record for the documents with the given keys.
func (ar *accessRecorder) CacheEvict(keys [][]byte) error {
	q := psql.RunWith(ar.dbCache).
		Delete(fqAccessRecordTable).
		Where(sq.Eq{keyCol: keys})
	ar.logger.Debug("evicting access records", logAccessRecordDelete(q, keys)...)
	ctx, cancel := context.WithTimeout(context.Background(), ar.params.PutTimeout)
	defer cancel()
	r, err := ar.qr.DeleteExecContext(ctx, q)
	if err != nil {
		return err
	}
	errors2.MaybePanic(err) // should never happen w/ Postgres
	ar.logger.Debug("evicted access records", logAccessRecordDeleted(keys, r)...)
	return nil
}

// GetNextEvictions gets the next batch of keys for documents to evict.
func (ar *accessRecorder) GetNextEvictions() ([][]byte, error) {
	beforeMin := time.Now().Unix()/60 - int64(ar.params.RecentWindowDays)*storage.MinsPerDay
	q := addEvictableClauses(beforeMin, psql.RunWith(ar.dbCache).Select(count))
	ar.logger.Debug("finding evictable values", logEvictableQuery(q, beforeMin)...)
	ctx, cancel := context.WithTimeout(context.Background(), ar.params.GetTimeout)
	row := ar.qr.SelectQueryRowContext(ctx, q)
	cancel()
	var nEvictable int
	if err := row.Scan(&nEvictable); err != nil {
		return nil, err
	}
	if nEvictable <= int(ar.params.LRUCacheSize) {
		// don't evict anything since cache size smaller than size limit
		ar.logger.Debug("fewer evictable values than cache size",
			nextEvictionsFields(nEvictable, ar.params.LRUCacheSize)...)
		return [][]byte{}, nil
	}
	nToEvict := nEvictable - int(ar.params.LRUCacheSize)
	if nToEvict > int(ar.params.EvictionBatchSize) {
		nToEvict = int(ar.params.EvictionBatchSize)
	}
	ar.logger.Debug("has evictable values",
		evictableValuesFields(nEvictable, nToEvict, ar.params)...)
	q = addEvictableClauses(beforeMin, psql.RunWith(ar.dbCache).
		Select(tbl1Col(keyCol)).
		OrderBy(tbl1Col(cachePutTimeMinCol)).
		Limit(uint64(nToEvict)),
	)
	ar.logger.Debug("getting keys to evict", logEvictKeys(q, nToEvict)...)
	ctx, cancel = context.WithTimeout(context.Background(), ar.params.GetTimeout)
	rows, err := ar.qr.SelectQueryContext(ctx, q)
	cancel()
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, nEvictable)
	i := 0
	for rows.Next() {
		if err2 := rows.Scan(&keys[i]); err2 != nil {
			return nil, err2
		}
		i++
	}
	if err2 := rows.Err(); err2 != nil {
		return nil, err2
	}
	if err2 := rows.Close(); err2 != nil {
		return nil, err2
	}

	ar.logger.Debug("found evictable values", nextEvictionsFields(i, ar.params.LRUCacheSize)...)
	return keys[:i], nil
}

func (ar *accessRecorder) recordAccess(key []byte, at accessType, skipOnConflict bool) error {
	q := psql.RunWith(ar.dbCache).
		Insert(fqAccessRecordTable).
		SetMap(getAccessRecordStmtValues(key, at))
	if skipOnConflict {
		q = q.Suffix(onConflictDoNothing)
	}
	ar.logger.Debug("inserting access record", logAccessRecordInsert(q, at)...)
	ctx, cancel := context.WithTimeout(context.Background(), ar.params.PutTimeout)
	r, err := ar.qr.InsertExecContext(ctx, q)
	cancel()
	if err != nil {
		return err
	}
	nRows, err := r.RowsAffected()
	errors2.MaybePanic(err) // should never happen w/ Postgres
	if nRows == 0 {
		ar.logger.Debug("access record already exists", logKeyAccessType(key, at)...)
		return nil
	}
	ar.logger.Debug("inserted new access record", logKeyAccessType(key, at)...)
	return nil
}

func addEvictableClauses(beforeMin int64, b sq.SelectBuilder) sq.SelectBuilder {
	return b.From(fmt.Sprintf("%s AS %s", fqAccessRecordTable, tblAlias1)).
		Join(
			fmt.Sprintf("%s AS %s", fqAccessRecordTable, tblAlias2) + " " +
				fmt.Sprintf("ON %s = %s", tbl1Col(keyCol), tbl2Col(keyCol)),
		).
		Where(andJoin(
			fmt.Sprintf("%s <= %d", tbl1Col(cachePutTimeMinCol), beforeMin),
			tbl2Col(libriPutOccurredCol), // = TRUE
		))
}

func getAccessRecordStmtValues(key []byte, rt accessType) map[string]interface{} {
	nowMicro := time.Now().UnixNano() / 1E3
	switch rt {
	case cachePut:
		nowMin := nowMicro / 1E6 / 60
		return map[string]interface{}{
			keyCol:               key,
			cachePutOccurredCol:  true,
			cachePutTimeMinCol:   nowMin,
			cachePutTimeMicroCol: nowMicro,
		}
	case libriPut:
		return map[string]interface{}{
			keyCol:               key,
			libriPutOccurredCol:  true,
			libriPutTimeMicroCol: nowMicro,
		}
	case cacheGet:
		return map[string]interface{}{
			keyCol:               key,
			cacheGetOccurredCol:  true,
			cacheGetTimeMicroCol: nowMicro,
		}
	default:
		panic("should never get here")
	}
}

func andJoin(preds ...string) string {
	return strings.Join(preds, andSpace)
}

func notNull(expr string) string {
	return fmt.Sprintf("%s IS NOT NULL", expr)
}

func tbl1Col(col string) string {
	return fmt.Sprintf("%s.%s", tblAlias1, col)
}

func tbl2Col(col string) string {
	return fmt.Sprintf("%s.%s", tblAlias2, col)
}
