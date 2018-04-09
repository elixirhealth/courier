package postgres

import (
	"database/sql"
	"encoding/hex"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/drausin/libri/libri/common/errors"
	"github.com/dustin/go-humanize"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logSQL               = "sql"
	logArgs              = "args"
	logKey               = "key"
	logValueSize         = "value_size"
	logValueSizeHuman    = "value_size_human"
	logNKeys             = "n_keys"
	logNDeleted          = "n_deleted"
	logAccessType        = "access_type"
	logBeforeMin         = "before_min"
	logBeforeMinISO      = "before_min_iso"
	logNEvictable        = "n_evictable"
	logLRUCacheSize      = "lru_cache_size"
	logNToEvict          = "n_to_evict"
	logEvictionBatchSize = "eviction_batch_size"
)

func logAccessRecordInsert(q sq.InsertBuilder, at accessType) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.String(logAccessType, at.string()),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logAccessRecordDelete(q sq.DeleteBuilder, keys [][]byte) []zapcore.Field {
	qSQL, _, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.String(logSQL, qSQL),
		zap.Int(logNKeys, len(keys)),
	}
}

func logAccessRecordDeleted(keys [][]byte, r sql.Result) []zapcore.Field {
	nDeleted, err := r.RowsAffected()
	errors.MaybePanic(err) // should never happen w/ Postgres
	return []zapcore.Field{
		zap.Int(logNKeys, len(keys)),
		zap.Int64(logNDeleted, nDeleted),
	}
}

func logKeyAccessType(key []byte, at accessType) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logKey, hex.EncodeToString(key)),
		zap.String(logAccessType, at.string()),
	}
}

func logEvictableQuery(q sq.SelectBuilder, beforeMin int64) []zapcore.Field {
	t := time.Unix(beforeMin*60, 0)
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.Int64(logBeforeMin, beforeMin),
		zap.String(logBeforeMinISO, t.Format("2006-01-02T15:04:05Z07")),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logEvictKeys(q sq.SelectBuilder, nToEvict int) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.Int(logNToEvict, nToEvict),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logCacheGet(q sq.SelectBuilder, key []byte) []zapcore.Field {
	qSQL, args, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.String(logKey, hex.EncodeToString(key)),
		zap.String(logSQL, qSQL),
		zap.Array(logArgs, queryArgs(args)),
	}
}

func logKeyValue(key, value []byte) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logKey, hex.EncodeToString(key)),
		zap.Int(logValueSize, len(value)),
		zap.String(logValueSizeHuman, humanize.Bytes(uint64(len(value)))),
	}
}

func nextEvictionsFields(nEvictable int, lruCacheSize uint) []zapcore.Field {
	return []zapcore.Field{
		zap.Int(logNEvictable, nEvictable),
		zap.Uint(logLRUCacheSize, lruCacheSize),
	}
}

func evictableValuesFields(nEvictable, nToEvict int, p *storage.Parameters) []zapcore.Field {
	return []zapcore.Field{
		zap.Int(logNEvictable, nEvictable),
		zap.Int(logNToEvict, nToEvict),
		zap.Uint(logLRUCacheSize, p.LRUCacheSize),
		zap.Uint(logEvictionBatchSize, p.EvictionBatchSize),
	}
}

func logCacheEvict(q sq.DeleteBuilder, keys [][]byte) []zapcore.Field {
	qSQL, _, err := q.ToSql()
	errors.MaybePanic(err)
	return []zapcore.Field{
		zap.String(logSQL, qSQL),
		zap.Int(logNKeys, len(keys)),
	}
}

type queryArgs []interface{}

func (qas queryArgs) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, qa := range qas {
		switch val := qa.(type) {
		case string:
			enc.AppendString(val)
		default:
			if err := enc.AppendReflected(qa); err != nil {
				return err
			}
		}
	}
	return nil
}
