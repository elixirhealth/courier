package datastore

import (
	"time"

	"github.com/elixirhealth/courier/pkg/server/storage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logKey           = "key"
	logValue         = "value"
	logFrom          = "from"
	logTo            = "to"
	logBeforeDate    = "before_date"
	logBeforeDateISO = "before_date_iso"
	logNEvictable    = "n_evictable"
	logNEvicted      = "n_evicted"
	logNToEvict      = "n_to_evict"

	logLRUCacheSize      = "lru_cache_size"
	logEvictionBatchSize = "eviction_batch_size"
	logNValues           = "n_values"
)

func nextEvictionsFields(nEvictable int, lruCacheSize uint) []zapcore.Field {
	return []zapcore.Field{
		zap.Int(logNEvictable, nEvictable),
		zap.Uint(logLRUCacheSize, lruCacheSize),
	}
}

func updatedAccessRecordFields(
	key string, existing, updated *storage.AccessRecord,
) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logKey, key),
		zap.Object(logFrom, existing),
		zap.Object(logTo, updated),
	}
}

func beforeDateFields(beforeDate int64) []zapcore.Field {
	beforeDateStr := time.Unix(beforeDate*storage.SecsPerDay, 0).Format("2006-01-02")
	return []zapcore.Field{
		zap.Int64(logBeforeDate, beforeDate),
		zap.String(logBeforeDateISO, beforeDateStr),
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

func accessRecordFields(key string, value *storage.AccessRecord) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logKey, key),
		zap.Object(logValue, value),
	}
}
