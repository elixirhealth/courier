package cache

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logKey                     = "key"
	logValue                   = "value"
	logCachePutDateEarliest    = "cache_put_date_earliest"
	logCachePutDateEarliestISO = "cache_put_date_earliest_iso"
	logCachePutTimeEarlist     = "cache_put_time_earliest"
	logLibriPutOccurred        = "libri_put_occurred"
	logLibriPutTimeEarliest    = "libri_put_time_earliest"
	logCacheGetTimeLatest      = "cache_get_time_latest"
	logFrom                    = "from"
	logTo                      = "to"
	logNValues                 = "n_values"
	logBeforeDate              = "before_date"
	logBeforeDateISO           = "before_date_iso"
	logNEvictable              = "n_evictable"
	logNEvicted                = "n_evicted"
	logNToEvict                = "n_to_evict"

	logStorageType          = "storage_type"
	logRecentWindowDays     = "recent_window_days"
	logLRUCacheSize         = "lru_cache_size"
	logEvictionBatchSize    = "eviction_batch_size"
	logEvictionPeriod       = "eviction_period"
	logEvictionQueryTimeout = "eviction_query_timeout"
	logGetTimeout           = "get_timeout"
	logPutTimeout           = "put_timeout"
	logDeleteTimeout        = "delete_timeout"
)

func nextEvictionsFields(nEvictable int, lruCacheSize uint) []zapcore.Field {
	return []zapcore.Field{
		zap.Int(logNEvictable, nEvictable),
		zap.Uint(logLRUCacheSize, lruCacheSize),
	}
}

func updatedAccessRecordFields(key string, existing, updated *AccessRecord) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logKey, key),
		zap.Object(logFrom, existing),
		zap.Object(logTo, updated),
	}
}

func beforeDateFields(beforeDate int64) []zapcore.Field {
	beforeDateStr := time.Unix(beforeDate*secsPerDay, 0).Format("2006-01-02")
	return []zapcore.Field{
		zap.Int64(logBeforeDate, beforeDate),
		zap.String(logBeforeDateISO, beforeDateStr),
	}
}

func evictableValuesFields(nEvictable, nToEvict int, p *Parameters) []zapcore.Field {
	return []zapcore.Field{
		zap.Int(logNEvictable, nEvictable),
		zap.Int(logNToEvict, nToEvict),
		zap.Uint(logLRUCacheSize, p.LRUCacheSize),
		zap.Uint(logEvictionBatchSize, p.EvictionBatchSize),
	}
}

func accessRecordFields(key string, value *AccessRecord) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logKey, key),
		zap.Object(logValue, value),
	}
}
