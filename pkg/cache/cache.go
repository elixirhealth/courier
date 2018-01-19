package cache

import (
	"time"

	"go.uber.org/zap/zapcore"
)

// StorageType indicates how the Cache is stored.
type StorageType int

const (
	// Unspecified indicates when the storage type is not specified (and thus should take the
	// default value).
	Unspecified StorageType = iota

	// InMemory indicates an ephemeral, in-memory (and thus not highly available) Cache. This
	// storage layer should generally only be used during testing and not in production.
	InMemory

	// DataStore indicates a (highly available) Cache backed by GCP DataStore.
	DataStore
)

const (
	defaultStorage              = InMemory
	defaultRecentWindow         = time.Hour * 24 * 7
	defaultLRUCacheSize         = 1e4
	defaultEvictionBatchSize    = uint(100)
	defaultEvictionPeriod       = 30 * time.Minute
	defaultEvictionQueryTimeout = 5 * time.Second
)

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

	// GetNextEvictions gets the next batch of keys for documents to evict, which is determines
	// by documents satisfying
	// - before recent window
	// - put into libri
	// - gotten least recently
	GetNextEvictions() ([]string, error)

	Evict(keys []string) error
}

// Parameters defines the parameters used by the cache implementation.
type Parameters struct {
	StorageType          StorageType
	RecentWindow         time.Duration
	LRUCacheSize         uint
	EvictionBatchSize    uint
	EvictionPeriod       time.Duration
	EvictionQueryTimeout time.Duration
}

// NewDefaultParameters returns a new instance of default cache parameter values.
func NewDefaultParameters() *Parameters {
	return &Parameters{
		StorageType:          defaultStorage,
		RecentWindow:         defaultRecentWindow,
		LRUCacheSize:         defaultLRUCacheSize,
		EvictionBatchSize:    defaultEvictionBatchSize,
		EvictionPeriod:       defaultEvictionPeriod,
		EvictionQueryTimeout: defaultEvictionQueryTimeout,
	}
}

// AccessRecord contains access times for Puts and Gets for a particular document. It is only
// exported so the DataStore API can reflect on it.
type AccessRecord struct {
	CachePutDateEarliest int64     `datastore:"cache_put_date_earliest"`
	CachePutTimeEarliest time.Time `datastore:"cache_put_time_earliest,noindex"`
	LibriPutOccurred     bool      `datastore:"libri_put_occurred"`
	LibriPutTimeEarliest time.Time `datastore:"libri_put_time_earliest,noindex"`
	CacheGetTimeLatest   time.Time `datastore:"cache_get_time_latest,noindex"`
}

func newCachePutAccessRecord() *AccessRecord {
	now := time.Now()
	return &AccessRecord{
		CachePutDateEarliest: now.Unix() / secsPerDay,
		CachePutTimeEarliest: now,
		LibriPutOccurred:     false,
	}
}

// MarshalLogObject marshals to the access record to the object encoder.
func (r *AccessRecord) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	oe.AddInt64(logCachePutDateEarliest, r.CachePutDateEarliest)
	oe.AddString(logCachePutDateEarliestISO,
		time.Unix(r.CachePutDateEarliest*secsPerDay, 0).Format("2006-01-02"))
	oe.AddTime(logCachePutTimeEarlist, r.CachePutTimeEarliest)
	oe.AddBool(logLibriPutOccurred, r.LibriPutOccurred)
	oe.AddTime(logLibriPutTimeEarliest, r.LibriPutTimeEarliest)
	oe.AddTime(logCacheGetTimeLatest, r.CacheGetTimeLatest)
	return nil
}

// keyGetTimes is a max-heap of keyGetTime objects sorted by getTime
type keyGetTimes []keyGetTime

type keyGetTime struct {
	key     string
	getTime time.Time
}

func (kgt keyGetTimes) Len() int {
	return len(kgt)
}

func (kgt keyGetTimes) Less(i, j int) bool {
	// After instead of Before turns min-heap into max-heap
	return kgt[i].getTime.After(kgt[j].getTime)
}

func (kgt keyGetTimes) Swap(i, j int) {
	kgt[i], kgt[j] = kgt[j], kgt[i]
}

func (kgt *keyGetTimes) Push(x interface{}) {
	*kgt = append(*kgt, x.(keyGetTime))
}

func (kgt *keyGetTimes) Pop() interface{} {
	old := *kgt
	n := len(old)
	x := old[n-1]
	*kgt = old[0 : n-1]
	return x
}

func (kgt keyGetTimes) Peak() keyGetTime {
	return kgt[0]
}
