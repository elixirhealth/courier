package cache

import "time"

// Cache stores documents in a quasi-LRU cache. Implementations of this interface define how the
// storage layer works.
type Cache interface {

	// Put stores the marshaled document value at the hex of its key.
	Put(key string, value []byte) error

	// Get retrieves the marshaled document value of the given key.
	Get(key string) ([]byte, error)

	// EvictNext removes the next batch of documents eligible for eviction from the cache.
	EvictNext() error
}

// AccessRecorder records put and get access to a particular document.
type AccessRecorder interface {

	// CachePut creates a new access record with the cache's put time for the document with
	// the given key.
	CachePut(key string) error

	// CacheGet updates the access record's latest get time for the document with the given key.
	CacheGet(key string) error

	// CacheEvict deletes the access record for the documents with the given keys.
	CacheEvict(keys []string) error

	// LibriPut updates the access record's latest libri put time.
	LibriPut(key string) error

	// GetNextEvictions gets the next batch of keys for documents to evict.
	GetNextEvictions() ([]string, error)
}

// Parameters defines the parameters used by the cache implementation.
type Parameters struct {
	RecentWindow         time.Duration
	EvictionBatchSize    uint
	EvictionPeriod       time.Duration
	EvictionQueryTimeout time.Duration
}
