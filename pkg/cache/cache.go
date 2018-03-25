package cache

import (
	"time"

	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultStorage is the default storage type.
	DefaultStorage = bstorage.Memory

	// DefaultRecentWindowDays is the default number of days in the recent window.
	DefaultRecentWindowDays = 7

	// DefaultLRUCacheSize is the default size of the LRU cache.
	DefaultLRUCacheSize = 1e4

	// DefaultEvictionBatchSize is the default size of each eviction batch.
	DefaultEvictionBatchSize = uint(100)

	// DefaultEvictionPeriod is the default period of evictions.
	DefaultEvictionPeriod = 30 * time.Minute

	// DefaultEvictionQueryTimeout is the default timeout for eviction queries.
	DefaultEvictionQueryTimeout = 5 * time.Second

	// DefaultCRUDTimeout is the default timeout for get, put, and delete DataStore operations.
	DefaultCRUDTimeout = 1 * time.Second
)

// Cache stores documents in a quasi-LRU cache. Implementations of this interface define how the
// bstorage layer works.
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
	Type                 bstorage.Type
	RecentWindowDays     int
	LRUCacheSize         uint
	EvictionBatchSize    uint
	EvictionPeriod       time.Duration
	GetQueryTimeout      time.Duration
	EvictionQueryTimeout time.Duration
	GetTimeout           time.Duration
	PutTimeout           time.Duration
	DeleteTimeout        time.Duration
}

// MarshalLogObject writes the params to the given object encoder.
func (p *Parameters) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	oe.AddString(logStorageType, p.Type.String())
	oe.AddInt(logRecentWindowDays, p.RecentWindowDays)
	oe.AddUint(logLRUCacheSize, p.LRUCacheSize)
	oe.AddUint(logEvictionBatchSize, p.EvictionBatchSize)
	oe.AddDuration(logEvictionPeriod, p.EvictionPeriod)
	oe.AddDuration(logEvictionQueryTimeout, p.EvictionQueryTimeout)
	oe.AddDuration(logGetTimeout, p.GetTimeout)
	oe.AddDuration(logPutTimeout, p.PutTimeout)
	oe.AddDuration(logDeleteTimeout, p.DeleteTimeout)
	return nil
}

// NewDefaultParameters returns a new instance of default cache parameter values.
func NewDefaultParameters() *Parameters {
	return &Parameters{
		Type:                 DefaultStorage,
		RecentWindowDays:     DefaultRecentWindowDays,
		LRUCacheSize:         DefaultLRUCacheSize,
		EvictionBatchSize:    DefaultEvictionBatchSize,
		EvictionPeriod:       DefaultEvictionPeriod,
		EvictionQueryTimeout: DefaultEvictionQueryTimeout,
		GetTimeout:           DefaultCRUDTimeout,
		PutTimeout:           DefaultCRUDTimeout,
		DeleteTimeout:        DefaultCRUDTimeout,
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
