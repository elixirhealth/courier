package memory

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/id"
	"github.com/drausin/libri/libri/common/logging"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/util"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMemoryCache_PutGet_ok(t *testing.T) {
	lg := zap.NewNop()
	rng := rand.New(rand.NewSource(0))
	params := storage.NewDefaultParameters()
	c, ar := New(params, lg)
	valueSizes := []int{1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024}

	for _, valueSize := range valueSizes {
		value1 := util.RandBytes(rng, valueSize)
		key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))

		err := c.Put(key, value1)
		assert.Nil(t, err)

		// put again just to see no-op
		err = c.Put(key, value1)
		assert.Nil(t, err)

		// check this internal side effect b/c it is important for eviction
		accessLogValue1 := ar.(*accessRecorder).records[key]
		assert.NotZero(t, accessLogValue1.CachePutTimeEarliest)
		assert.Zero(t, accessLogValue1.LibriPutTimeEarliest)
		assert.Zero(t, accessLogValue1.CacheGetTimeLatest)

		value2, err := c.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value1, value2)

		// check side effect again
		accessLogValue2 := ar.(*accessRecorder).records[key]
		assert.NotZero(t, accessLogValue2.CachePutTimeEarliest)
		assert.Equal(t, accessLogValue1.CachePutTimeEarliest, accessLogValue2.CachePutTimeEarliest)
		assert.Zero(t, accessLogValue2.LibriPutTimeEarliest)
		assert.NotZero(t, accessLogValue2.CacheGetTimeLatest)
	}
}

func TestMemoryCache_Put_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()

	// bad key
	c, _ := New(storage.NewDefaultParameters(), lg)
	err := c.Put("too short key", []byte{})
	assert.Equal(t, storage.ErrInvalidKeySize, err)

	// different values for same key
	c, _ = New(storage.NewDefaultParameters(), lg)
	key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))
	err = c.Put(key, []byte("value 1"))
	assert.Nil(t, err)
	err = c.Put(key, []byte("value 2"))
	assert.Equal(t, storage.ErrExistingNotEqualNewValue, err)

	// access recorder error
	ar := &fixedAccessRecorder{
		cachePutErr: errors.New("some cache put error"),
	}
	c = &cache{
		ar:     ar,
		docs:   make(map[string][]byte),
		logger: lg,
	}
	err = c.Put(key, []byte("value 1"))
	assert.NotNil(t, err)
}

func TestMemoryCache_Get_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()
	c, _ := New(storage.NewDefaultParameters(), lg)
	key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))

	// missing value for key
	value, err := c.Get(key)
	assert.Equal(t, storage.ErrMissingValue, err)
	assert.Nil(t, value)

	// access recorder cache get error
	ar := &fixedAccessRecorder{
		cacheGetErr: errors.New("some cache get error"),
	}
	c = &cache{
		ar:     ar,
		docs:   make(map[string][]byte),
		logger: lg,
	}
	err = c.Put(key, []byte("value 1"))
	assert.Nil(t, err)
	value, err = c.Get(key)
	assert.NotNil(t, err)
	assert.Nil(t, value)
}

func TestMemoryCache_EvictNext_ok(t *testing.T) {
	lg := zap.NewNop()
	evictionKeys := []string{"key1", "key2"}
	ar := &fixedAccessRecorder{
		nextEvictions: []string{},
	}
	c := &cache{
		ar:     ar,
		logger: lg,
	}
	err := c.EvictNext()
	assert.Nil(t, err)

	ar = &fixedAccessRecorder{
		nextEvictions: evictionKeys,
	}
	c = &cache{
		ar: ar,
		docs: map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
		},
		logger: lg,
	}
	err = c.EvictNext()
	assert.Nil(t, err)

	assert.Len(t, c.docs, 1)
	assert.Equal(t, evictionKeys, ar.cacheEvictKeys)
}

func TestMemoryCache_EvictNext_err(t *testing.T) {
	lg := zap.NewNop()
	// check GetNextEvictions error bubbles up
	ar := &fixedAccessRecorder{
		getEvictionBatchErr: errors.New("some error"),
	}
	c := &cache{
		ar:     ar,
		logger: lg,
	}
	err := c.EvictNext()
	assert.NotNil(t, err)

	// check ar.Evict error bubbes up
	ar = &fixedAccessRecorder{
		nextEvictions: []string{"key1", "key2"},
		cacheEvictErr: errors.New("some evict error"),
	}
	c = &cache{
		ar:     ar,
		logger: lg,
	}
	err = c.EvictNext()
	assert.NotNil(t, err)

	// check missing value error
	ar = &fixedAccessRecorder{
		nextEvictions: []string{"key1", "key2"},
	}
	c = &cache{
		ar:     ar,
		docs:   make(map[string][]byte),
		logger: lg,
	}
	err = c.EvictNext()
	assert.NotNil(t, err)
}

func TestMemoryAccessRecorder_CachePut(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	key := "some key"
	err := ar.CachePut(key)
	assert.Nil(t, err)
	assert.NotZero(t, ar.records[key].CachePutDateEarliest)
	assert.NotZero(t, ar.records[key].CachePutTimeEarliest)
	assert.Zero(t, ar.records[key].LibriPutTimeEarliest)
	assert.Zero(t, ar.records[key].CacheGetTimeLatest)
	assert.False(t, ar.records[key].LibriPutOccurred)

	// second put should be no-op
	err = ar.CachePut(key)
	assert.Nil(t, err)
}

func TestMemoryAccessRecorder_CacheGet(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	key := "some key"
	err := ar.CachePut(key)
	assert.Nil(t, err)

	err = ar.CacheGet(key)
	assert.Nil(t, err)
	assert.NotZero(t, ar.records[key].CachePutTimeEarliest)
	assert.Zero(t, ar.records[key].LibriPutTimeEarliest)
	assert.NotZero(t, ar.records[key].CacheGetTimeLatest)

	err = ar.CacheGet("some other key")
	assert.Equal(t, storage.ErrMissingValue, err)
}

func TestMemoryAccessRecorder_CacheEvict_ok(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		err := ar.CachePut(key)
		assert.Nil(t, err)
	}

	err := ar.CacheEvict([]string{"key1", "key2"})
	assert.Nil(t, err)

	err = ar.CacheGet("key1")
	assert.Equal(t, storage.ErrMissingValue, err)
	err = ar.CacheGet("key2")
	assert.Equal(t, storage.ErrMissingValue, err)
	err = ar.CacheGet("key3")
	assert.Nil(t, err)
}

func TestMemoryAccessRecorder_CacheEvict_err(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	err := ar.CacheEvict([]string{"key1", "key2"})
	assert.Equal(t, storage.ErrMissingValue, err)
}

func TestMemoryAccessRecorder_LibriPut(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	key := "some key"
	err := ar.CachePut(key)
	assert.Nil(t, err)

	err = ar.LibriPut(key)
	assert.Nil(t, err)
	assert.NotZero(t, ar.records[key].CachePutTimeEarliest)
	assert.NotZero(t, ar.records[key].LibriPutTimeEarliest)
	assert.True(t, ar.records[key].LibriPutOccurred)
	assert.Zero(t, ar.records[key].CacheGetTimeLatest)

	err = ar.LibriPut("some other key")
	assert.Equal(t, storage.ErrMissingValue, err)
}

func TestGetNextEvictions(t *testing.T) {
	lg := logging.NewDevLogger(zapcore.DebugLevel)
	now := time.Now()
	ar := &accessRecorder{
		params: &storage.Parameters{
			RecentWindowDays:  1,
			EvictionBatchSize: 2,
			LRUCacheSize:      2,
		},
		records: map[string]*storage.AccessRecord{
			// not evicted b/c put too recently
			"key1": {
				CachePutDateEarliest: timeToDate(now.Add(-2 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-time.Second),
				LibriPutOccurred:     true,
			},
			// not evicted b/c libri put hasn't occurred
			"key2": {
				CachePutDateEarliest: timeToDate(now.Add(-36 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-time.Second),
				LibriPutOccurred:     false,
			},
			// evictable & evicted in first batch
			"key3": {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-6 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable & evicted in first batch
			"key4": {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-5 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable & evicted in second batch
			"key5": {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-4 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable but not evicted b/c of LRU cache
			"key6": {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-3 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable but not evicted b/c of LRU cache
			"key7": {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-2 * time.Second),
				LibriPutOccurred:     true,
			},
		},
		logger: lg,
	}

	keys, err := ar.GetNextEvictions()
	assert.Nil(t, err)
	sort.Strings(keys)
	assert.Equal(t, []string{"key3", "key4"}, keys)

	ar.CacheEvict(keys)
	keys, err = ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Equal(t, []string{"key5"}, keys)

	ar.CacheEvict(keys)
	keys, err = ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Len(t, keys, 0)

}

func timeToDate(t time.Time) int64 {
	return t.Unix() / storage.SecsPerDay
}

func TestMemoryAccessRecorder_Evict_ok(t *testing.T) {
	lg := zap.NewNop()
	keys := []string{"key1", "key2"}
	ds := accessRecorder{
		records: map[string]*storage.AccessRecord{
			"key1": storage.NewCachePutAccessRecord(),
			"key2": storage.NewCachePutAccessRecord(),
		},
		logger: lg,
	}
	err := ds.CacheEvict(keys)
	assert.Nil(t, err)
	assert.Len(t, ds.records, 0)
}

func TestMemoryAccessRecorder_Evict_err(t *testing.T) {
	lg := zap.NewNop()
	keys := []string{"key1", "key2"}
	ds := accessRecorder{
		records: map[string]*storage.AccessRecord{},
		logger:  lg,
	}
	err := ds.CacheEvict(keys)
	assert.Equal(t, storage.ErrMissingValue, err)
}

type fixedAccessRecorder struct {
	cachePutErr         error
	cacheGetErr         error
	cacheEvictErr       error
	cacheEvictKeys      []string
	libriPutErr         error
	nextEvictions       []string
	getEvictionBatchErr error
}

func (r *fixedAccessRecorder) CachePut(key string) error {
	return r.cachePutErr
}

func (r *fixedAccessRecorder) CacheGet(key string) error {
	return r.cacheGetErr
}

func (r *fixedAccessRecorder) CacheEvict(keys []string) error {
	r.cacheEvictKeys = keys
	return r.cacheEvictErr
}

func (r *fixedAccessRecorder) LibriPut(key string) error {
	return r.libriPutErr
}

func (r *fixedAccessRecorder) GetNextEvictions() ([]string, error) {
	return r.nextEvictions, r.getEvictionBatchErr
}
