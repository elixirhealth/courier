package memory

import (
	"container/heap"
	"sync"
	"time"

	"github.com/elixirhealth/courier/pkg/server/storage"
	"go.uber.org/zap"
)

type accessRecorder struct {
	records map[string]*storage.AccessRecord
	params  *storage.Parameters
	mu      sync.Mutex
	logger  *zap.Logger
}

func (r *accessRecorder) CachePut(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[key] = storage.NewCachePutAccessRecord()
	r.logger.Debug("put new access record", accessRecordFields(key, r.records[key])...)
	return nil
}

func (r *accessRecorder) CacheGet(key string) error {
	return r.update(key, &storage.AccessRecord{CacheGetTimeLatest: time.Now()})
}

func (r *accessRecorder) CacheEvict(keys []string) error {
	for _, key := range keys {
		r.mu.Lock()
		if _, in := r.records[key]; !in {
			r.mu.Unlock()
			return storage.ErrMissingValue
		}
		delete(r.records, key)
		r.mu.Unlock()
	}
	r.logger.Debug("evicted access records", zap.Int(logNValues, len(keys)))
	return nil
}

func (r *accessRecorder) LibriPut(key string) error {
	return r.update(key, &storage.AccessRecord{
		LibriPutOccurred:     true,
		LibriPutTimeEarliest: time.Now(),
	})
}

func (r *accessRecorder) GetNextEvictions() ([]string, error) {
	// not trying to be efficient, so just do full scan for docs satisfying eviction criteria
	evict := &storage.KeyGetTimes{}
	beforeDate := time.Now().Unix()/storage.SecsPerDay - int64(r.params.RecentWindowDays)
	r.logger.Debug("finding evictable values", beforeDateFields(beforeDate)...)
	r.mu.Lock()
	nEvictable := 0
	for key, record := range r.records {
		if record.CachePutDateEarliest < beforeDate && record.LibriPutOccurred {
			heap.Push(evict, storage.KeyGetTime{
				Key:     key,
				GetTime: record.CacheGetTimeLatest,
			})
			nEvictable++
		}
		if uint(evict.Len()) > r.params.EvictionBatchSize {
			heap.Pop(evict)
		}
	}
	r.mu.Unlock()
	if nEvictable <= int(r.params.LRUCacheSize) {
		// don't evict anything since cache size smaller than size limit
		r.logger.Debug("fewer evictable values than cache size",
			nextEvictionsFields(nEvictable, r.params.LRUCacheSize)...)
		return []string{}, nil
	}
	nToEvict := nEvictable - int(r.params.LRUCacheSize)
	if nToEvict > int(r.params.EvictionBatchSize) {
		nToEvict = int(r.params.EvictionBatchSize)
	}
	r.logger.Debug("has evictable values",
		evictableValuesFields(nEvictable, nToEvict, r.params)...)
	for evict.Len() > nToEvict {
		heap.Pop(evict)
	}

	evictKeys := make([]string, evict.Len())
	for i, kgt := range *evict {
		evictKeys[i] = kgt.Key
	}
	r.logger.Debug("found evictable values",
		nextEvictionsFields(evict.Len(), r.params.LRUCacheSize)...)
	return evictKeys, nil
}

func (r *accessRecorder) update(key string, update *storage.AccessRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, in := r.records[key]
	if !in {
		return storage.ErrMissingValue
	}
	updated := *existing
	storage.UpdateAccessRecord(&updated, update)
	r.records[key] = &updated
	r.logger.Debug("updated access record",
		updatedAccessRecordFields(key, existing, &updated)...)
	return nil
}
