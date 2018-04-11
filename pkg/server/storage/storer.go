package storage

import (
	"errors"
	"time"

	"github.com/drausin/libri/libri/common/id"
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

	// KeySize is size of the length of a document ID
	KeySize = id.Length

	// MaxValueSize is the maximum allowed size of a serialized document value.
	MaxValueSize = 2*1024*1024 + 1024

	// MinsPerDay is the number of minutes in a day.
	MinsPerDay = 60 * 24

	// SecsPerDay is the number of seconds in a day.
	SecsPerDay = 60 * 60 * 24
)

var (
	// ErrMissingValue indicates that a value is missing for a given Key.
	ErrMissingValue = errors.New("missing value")

	// ErrInvalidKeySize indicates when the Key is not the expected length.
	ErrInvalidKeySize = errors.New("invalid Key size")

	// ErrValueTooLarge indicates when the value is too large to be stored.
	ErrValueTooLarge = errors.New("value too large")

	// ErrExistingNotEqualNewValue indicates when the existing stored value is not the same as
	// the new value,
	// violating the cache's immutability assumption.
	ErrExistingNotEqualNewValue = errors.New("existing value does not equal new value")
)

// Cache stores documents in a quasi-LRU cache. Implementations of this interface define how the
// bstorage layer works.
type Cache interface {
	// Put stores the marshaled document value at the hex of its Key. It returns a boolean flag
	// for whether the key existed already.
	Put(key []byte, value []byte) (bool, error)

	// Get retrieves the marshaled document value of the given Key.
	Get(key []byte) ([]byte, error)

	// EvictNext removes the next batch of documents eligible for eviction from the cache.
	EvictNext() error

	// Close cleans up an resources held by the cache.
	Close() error
}

// AccessRecorder records put and get access to a particular document.
type AccessRecorder interface {
	// CachePut creates a new access record with the cache's put time for the document with
	// the given Key.
	CachePut(key []byte) error

	// LibriPut updates the access record's latest libri put time.
	LibriPut(key []byte) error

	// CacheGet updates the access record's latest get time for the document with the given Key.
	CacheGet(key []byte) error

	// CacheEvict deletes the access record for the documents with the given keys.
	CacheEvict(keys [][]byte) error

	// GetNextEvictions gets the next batch of keys for documents to evict, which is determines
	// by documents satisfying
	// - before recent window
	// - put into libri
	// - gotten least recently
	GetNextEvictions() ([][]byte, error)
}

// Parameters defines the parameters used by the cache implementation.
type Parameters struct {
	Type                 bstorage.Type
	RecentWindowDays     int
	LRUCacheSize         uint
	EvictionBatchSize    uint
	EvictionPeriod       time.Duration
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

// AccessRecord contains access times for Puts and Gets for a particular document.
type AccessRecord struct {
	CachePutDateEarliest int64     `datastore:"cache_put_date_earliest"`
	CachePutTimeEarliest time.Time `datastore:"cache_put_time_earliest,noindex"`
	LibriPutOccurred     bool      `datastore:"libri_put_occurred"`
	LibriPutTimeEarliest time.Time `datastore:"libri_put_time_earliest,noindex"`
	CacheGetTimeLatest   time.Time `datastore:"cache_get_time_latest,noindex"`
}

// NewCachePutAccessRecord creates a new *AccessRecord representing a cache put.
func NewCachePutAccessRecord() *AccessRecord {
	now := time.Now()
	return &AccessRecord{
		CachePutDateEarliest: now.Unix() / SecsPerDay,
		CachePutTimeEarliest: now,
		LibriPutOccurred:     false,
	}
}

// UpdateAccessRecord updates the existing access record with the non-zero values of a new access
// record.
func UpdateAccessRecord(existing, update *AccessRecord) {
	if !update.CacheGetTimeLatest.IsZero() {
		existing.CacheGetTimeLatest = update.CacheGetTimeLatest
	}
	if !update.LibriPutTimeEarliest.IsZero() {
		existing.LibriPutTimeEarliest = update.LibriPutTimeEarliest
	}
	if update.LibriPutOccurred {
		existing.LibriPutOccurred = update.LibriPutOccurred
	}
}

// MarshalLogObject marshals to the access record to the object encoder.
func (r *AccessRecord) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	oe.AddInt64(logCachePutDateEarliest, r.CachePutDateEarliest)
	oe.AddString(logCachePutDateEarliestISO,
		time.Unix(r.CachePutDateEarliest*SecsPerDay, 0).Format("2006-01-02"))
	oe.AddTime(logCachePutTimeEarlist, r.CachePutTimeEarliest)
	oe.AddBool(logLibriPutOccurred, r.LibriPutOccurred)
	oe.AddTime(logLibriPutTimeEarliest, r.LibriPutTimeEarliest)
	oe.AddTime(logCacheGetTimeLatest, r.CacheGetTimeLatest)
	return nil
}

// KeyGetTime represents get access time for a given key.
type KeyGetTime struct {
	Key     []byte
	GetTime time.Time
}

// KeyGetTimes is a max-heap of KeyGetTime objects sorted by GetTime
type KeyGetTimes []KeyGetTime

// Len is the number of key get times.
func (kgt KeyGetTimes) Len() int {
	return len(kgt)
}

// Less returns whether key get time i comes after that of j.
func (kgt KeyGetTimes) Less(i, j int) bool {
	// After instead of Before turns min-heap into max-heap
	return kgt[i].GetTime.After(kgt[j].GetTime)
}

// Swap swaps ket get times i & j.
func (kgt KeyGetTimes) Swap(i, j int) {
	kgt[i], kgt[j] = kgt[j], kgt[i]
}

// Push adds a key get time to the heap.
func (kgt *KeyGetTimes) Push(x interface{}) {
	*kgt = append(*kgt, x.(KeyGetTime))
}

// Pop removes and returns the key get time from the root of the heap.
func (kgt *KeyGetTimes) Pop() interface{} {
	old := *kgt
	n := len(old)
	x := old[n-1]
	*kgt = old[0 : n-1]
	return x
}

// Peak returns the key get time from the root of the heap.
func (kgt KeyGetTimes) Peak() KeyGetTime {
	return kgt[0]
}
