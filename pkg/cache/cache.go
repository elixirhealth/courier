package cache

// Cache stores documents in a quasi-LRU cache. Implementations of this interface define how the
// storage layer works.
type Cache interface {

	// put stores the marshaled document value at the hex of its key.
	Put(key string, value []byte) error

	// get retrieves the marshaled document value of the given hex key.
	Get(key string) ([]byte, error)

	// TODO (drausin) eviction
}

// AccessRecorder records put and get access to a particular document.
type AccessRecorder interface {

	// CachePut creates a new access record with the cache's put time for the document with
	// the given key.
	CachePut(key string) error

	// CacheGet updates the access record's latest get time for the document with the given key.
	CacheGet(key string) error

	// LibriPut updates the access record's latest libri put time.
	LibriPut(key string) error

	// TODO (drausin) docs to evict
}
