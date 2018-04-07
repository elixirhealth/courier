package memory

import (
	"container/heap"
	"encoding/hex"
	"sync"
	"time"

	"github.com/drausin/libri/libri/common/errors"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"go.uber.org/zap"
)

type accessRecorder struct {
	records map[string]*storage.AccessRecord
	params  *storage.Parameters
	mu      sync.Mutex
	logger  *zap.Logger
}

func (r *accessRecorder) CachePut(key []byte) error {
	keyHex := hex.EncodeToString(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[keyHex] = storage.NewCachePutAccessRecord()
	r.logger.Debug("put new access record", accessRecordFields(key, r.records[keyHex])...)
	return nil
}

func (r *accessRecorder) CacheGet(key []byte) error {
	return r.update(key, &storage.AccessRecord{CacheGetTimeLatest: time.Now()})
}

func (r *accessRecorder) CacheEvict(keys [][]byte) error {
	for _, key := range keys {
		keyHex := hex.EncodeToString(key)
		r.mu.Lock()
		if _, in := r.records[keyHex]; !in {
			r.mu.Unlock()
			return storage.ErrMissingValue
		}
		delete(r.records, keyHex)
		r.mu.Unlock()
	}
	r.logger.Debug("evicted access records", zap.Int(logNValues, len(keys)))
	return nil
}

func (r *accessRecorder) LibriPut(key []byte) error {
	return r.update(key, &storage.AccessRecord{
		LibriPutOccurred:     true,
		LibriPutTimeEarliest: time.Now(),
	})
}

func (r *accessRecorder) GetNextEvictions() ([][]byte, error) {
	// not trying to be efficient, so just do full scan for docs satisfying eviction criteria
	evict := &storage.KeyGetTimes{}
	beforeDate := time.Now().Unix()/storage.SecsPerDay - int64(r.params.RecentWindowDays)
	r.logger.Debug("finding evictable values", beforeDateFields(beforeDate)...)
	r.mu.Lock()
	nEvictable := 0
	for keyHex, record := range r.records {
		key, err := hex.DecodeString(keyHex)
		errors.MaybePanic(err) // should never happen
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
		return [][]byte{}, nil
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

	evictKeys := make([][]byte, evict.Len())
	for i, kgt := range *evict {
		evictKeys[i] = kgt.Key
	}
	r.logger.Debug("found evictable values",
		nextEvictionsFields(evict.Len(), r.params.LRUCacheSize)...)
	return evictKeys, nil
}

func (r *accessRecorder) update(key []byte, update *storage.AccessRecord) error {
	keyHex := hex.EncodeToString(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, in := r.records[keyHex]
	if !in {
		return storage.ErrMissingValue
	}
	updated := *existing
	storage.UpdateAccessRecord(&updated, update)
	r.records[keyHex] = &updated
	r.logger.Debug("updated access record",
		updatedAccessRecordFields(key, existing, &updated)...)
	return nil
}
