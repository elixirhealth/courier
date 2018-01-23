package cache

import (
	"bytes"
	"container/heap"
	"sync"
	"time"

	"go.uber.org/zap"
)

type memoryCache struct {
	accessRecorder AccessRecorder
	docs           map[string][]byte
	mu             sync.Mutex
	logger         *zap.Logger
}

// NewMemory creates a new in-memory cache with the given parameters.
func NewMemory(params *Parameters, logger *zap.Logger) (Cache, AccessRecorder) {
	ar := &memoryAccessRecorder{
		records: make(map[string]*AccessRecord),
		params:  params,
		logger:  logger,
	}
	return &memoryCache{
		accessRecorder: ar,
		docs:           make(map[string][]byte),
		logger:         logger,
	}, ar
}

// Put stores the marshaled document value at the hex of its key.
func (c *memoryCache) Put(key string, value []byte) error {
	logger := c.logger.With(zap.String("key", key))
	logger.Debug("putting into cache")
	if len(key) != keySize {
		return ErrInvalidKeySize
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, in := c.docs[key]; in {
		if !bytes.Equal(value, existing) {
			return ErrExistingNotEqualNewValue
		}
		logger.Debug("cache already contains value")
		return nil
	}
	c.docs[key] = value
	if err := c.accessRecorder.CachePut(key); err != nil {
		return err
	}
	logger.Debug("put into cache")
	return nil
}

// Get retrieves the marshaled document value of the given hex key.
func (c *memoryCache) Get(key string) ([]byte, error) {
	logger := c.logger.With(zap.String("key", key))
	logger.Debug("getting from cache")
	value, in := c.docs[key]
	if !in {
		return nil, ErrMissingValue
	}
	if err := c.accessRecorder.CacheGet(key); err != nil {
		return nil, err
	}
	logger.Debug("got value from cache")
	return value, nil
}

// EvictNext removes the next batch of documents eligible for eviction from the cache.
func (c *memoryCache) EvictNext() error {
	c.logger.Debug("beginning next eviction")
	keys, err := c.accessRecorder.GetNextEvictions()
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		c.logger.Debug("evicted no documents")
		return nil
	}
	if err = c.accessRecorder.Evict(keys); err != nil {
		return err
	}
	for _, key := range keys {
		c.mu.Lock()
		if _, in := c.docs[key]; !in {
			c.mu.Unlock()
			return ErrMissingValue
		}
		delete(c.docs, key)
		c.mu.Unlock()
	}
	c.logger.Info("evicted documents", zap.Int(logNEvicted, len(keys)))
	return nil
}

type memoryAccessRecorder struct {
	records map[string]*AccessRecord
	params  *Parameters
	mu      sync.Mutex
	logger  *zap.Logger
}

func (r *memoryAccessRecorder) CachePut(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[key] = newCachePutAccessRecord()
	r.logger.Debug("put new access record", accessRecordFields(key, r.records[key])...)
	return nil
}

func (r *memoryAccessRecorder) CacheGet(key string) error {
	return r.update(key, &AccessRecord{CacheGetTimeLatest: time.Now()})
}

func (r *memoryAccessRecorder) CacheEvict(keys []string) error {
	for _, key := range keys {
		r.mu.Lock()
		if _, in := r.records[key]; !in {
			r.mu.Unlock()
			return ErrMissingValue
		}
		delete(r.records, key)
		r.mu.Unlock()
	}
	r.logger.Debug("evicted access records", zap.Int(logNValues, len(keys)))
	return nil
}

func (r *memoryAccessRecorder) LibriPut(key string) error {
	return r.update(key, &AccessRecord{
		LibriPutOccurred:     true,
		LibriPutTimeEarliest: time.Now(),
	})
}

func (r *memoryAccessRecorder) GetNextEvictions() ([]string, error) {
	// not trying to be efficient, so just do full scan for docs satisfying eviction criteria
	evict := &keyGetTimes{}
	beforeDate := time.Now().Unix()/secsPerDay - int64(r.params.RecentWindowDays)
	r.logger.Debug("finding evictable values", beforeDateFields(beforeDate)...)
	r.mu.Lock()
	nEvictable := 0
	for key, record := range r.records {
		if record.CachePutDateEarliest < beforeDate && record.LibriPutOccurred {
			heap.Push(evict, keyGetTime{key: key, getTime: record.CacheGetTimeLatest})
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
		evictKeys[i] = kgt.key
	}
	r.logger.Debug("found evictable values",
		nextEvictionsFields(evict.Len(), r.params.LRUCacheSize)...)
	return evictKeys, nil
}

func (r *memoryAccessRecorder) Evict(keys []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range keys {
		if _, in := r.records[key]; !in {
			return ErrMissingValue
		}
		delete(r.records, key)
	}
	r.logger.Debug("evicted access records", zap.Int(logNEvicted, len(keys)))
	return nil
}

func (r *memoryAccessRecorder) update(key string, update *AccessRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, in := r.records[key]
	if !in {
		return ErrMissingValue
	}
	updated := *existing
	updateAccessRecord(&updated, update)
	r.records[key] = &updated
	r.logger.Debug("updated access record",
		updatedAccessRecordFields(key, existing, &updated)...)
	return nil
}
