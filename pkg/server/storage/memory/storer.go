package memory

import (
	"bytes"
	"container/heap"
	"sync"
	"time"

	"github.com/elixirhealth/courier/pkg/server/storage"
	"go.uber.org/zap"
)

type cache struct {
	ar     storage.AccessRecorder
	docs   map[string][]byte
	mu     sync.Mutex
	logger *zap.Logger
}

// New creates a new in-memory cache with the given parameters.
func New(params *storage.Parameters, logger *zap.Logger) (storage.Cache, storage.AccessRecorder) {
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		params:  params,
		logger:  logger,
	}
	return &cache{
		ar:     ar,
		docs:   make(map[string][]byte),
		logger: logger,
	}, ar
}

// Put stores the marshaled document value at the hex of its key.
func (c *cache) Put(key string, value []byte) error {
	logger := c.logger.With(zap.String("key", key))
	logger.Debug("putting into cache")
	if len(key) != storage.KeySize {
		return storage.ErrInvalidKeySize
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, in := c.docs[key]; in {
		if !bytes.Equal(value, existing) {
			return storage.ErrExistingNotEqualNewValue
		}
		logger.Debug("cache already contains value")
		return nil
	}
	c.docs[key] = value
	if err := c.ar.CachePut(key); err != nil {
		return err
	}
	logger.Debug("put into cache")
	return nil
}

// Get retrieves the marshaled document value of the given hex key.
func (c *cache) Get(key string) ([]byte, error) {
	logger := c.logger.With(zap.String("key", key))
	logger.Debug("getting from cache")
	c.mu.Lock()
	value, in := c.docs[key]
	c.mu.Unlock()
	if !in {
		return nil, storage.ErrMissingValue
	}
	if err := c.ar.CacheGet(key); err != nil {
		return nil, err
	}
	logger.Debug("got value from cache")
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
	for _, key := range keys {
		c.mu.Lock()
		if _, in := c.docs[key]; !in {
			c.mu.Unlock()
			return storage.ErrMissingValue
		}
		delete(c.docs, key)
		c.mu.Unlock()
	}
	c.logger.Info("evicted documents", zap.Int(logNEvicted, len(keys)))
	return nil
}

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
