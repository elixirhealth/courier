package cache

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/id"
	"github.com/elxirhealth/service-base/pkg/util"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMemoryCache_PutGet_ok(t *testing.T) {
	lg := zap.NewNop()
	rng := rand.New(rand.NewSource(0))
	params := NewDefaultParameters()
	c, ar := NewMemory(params, lg)
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
		accessLogValue1 := ar.(*memoryAccessRecorder).records[key]
		assert.NotZero(t, accessLogValue1.CachePutTimeEarliest)
		assert.Zero(t, accessLogValue1.LibriPutTimeEarliest)
		assert.Zero(t, accessLogValue1.CacheGetTimeLatest)

		value2, err := c.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value1, value2)

		// check side effect again
		accessLogValue2 := ar.(*memoryAccessRecorder).records[key]
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
	c, _ := NewMemory(NewDefaultParameters(), lg)
	err := c.Put("too short key", []byte{})
	assert.Equal(t, ErrInvalidKeySize, err)

	// different values for same key
	c, _ = NewMemory(NewDefaultParameters(), lg)
	key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))
	err = c.Put(key, []byte("value 1"))
	assert.Nil(t, err)
	err = c.Put(key, []byte("value 2"))
	assert.Equal(t, ErrExistingNotEqualNewValue, err)

	// access recorder error
	ar := &fixedAccessRecorder{
		cachePutErr: errors.New("some cache put error"),
	}
	c = &memoryCache{
		accessRecorder: ar,
		docs:           make(map[string][]byte),
		logger:         lg,
	}
	err = c.Put(key, []byte("value 1"))
	assert.NotNil(t, err)
}

func TestMemoryCache_Get_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()
	c, _ := NewMemory(NewDefaultParameters(), lg)
	key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))

	// missing value for key
	value, err := c.Get(key)
	assert.Equal(t, ErrMissingValue, err)
	assert.Nil(t, value)

	// access recorder cache get error
	ar := &fixedAccessRecorder{
		cacheGetErr: errors.New("some cache get error"),
	}
	c = &memoryCache{
		accessRecorder: ar,
		docs:           make(map[string][]byte),
		logger:         lg,
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
	c := &memoryCache{
		accessRecorder: ar,
		logger:         lg,
	}
	err := c.EvictNext()
	assert.Nil(t, err)

	ar = &fixedAccessRecorder{
		nextEvictions: evictionKeys,
	}
	c = &memoryCache{
		accessRecorder: ar,
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
	assert.Equal(t, evictionKeys, ar.evictKeys)
}

func TestMemoryCache_EvictNext_err(t *testing.T) {
	lg := zap.NewNop()
	// check GetNextEvictions error bubbles up
	ar := &fixedAccessRecorder{
		getEvictionBatchErr: errors.New("some error"),
	}
	c := &memoryCache{
		accessRecorder: ar,
		logger:         lg,
	}
	err := c.EvictNext()
	assert.NotNil(t, err)

	// check accessRecorder.Evict error bubbes up
	ar = &fixedAccessRecorder{
		nextEvictions: []string{"key1", "key2"},
		evictErr:      errors.New("some evict error"),
	}
	c = &memoryCache{
		accessRecorder: ar,
		logger:         lg,
	}
	err = c.EvictNext()
	assert.NotNil(t, err)

	// check missing value error
	ar = &fixedAccessRecorder{
		nextEvictions: []string{"key1", "key2"},
	}
	c = &memoryCache{
		accessRecorder: ar,
		docs:           make(map[string][]byte),
		logger:         lg,
	}
	err = c.EvictNext()
	assert.NotNil(t, err)
}

func TestMemoryAccessRecorder_CachePut(t *testing.T) {
	lg := zap.NewNop()
	ar := &memoryAccessRecorder{
		records: make(map[string]*AccessRecord),
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
	ar := &memoryAccessRecorder{
		records: make(map[string]*AccessRecord),
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
	assert.Equal(t, ErrMissingValue, err)
}

func TestMemoryAccessRecorder_CacheEvict_ok(t *testing.T) {
	lg := zap.NewNop()
	ar := &memoryAccessRecorder{
		records: make(map[string]*AccessRecord),
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
	assert.Equal(t, ErrMissingValue, err)
	err = ar.CacheGet("key2")
	assert.Equal(t, ErrMissingValue, err)
	err = ar.CacheGet("key3")
	assert.Nil(t, err)
}

func TestMemoryAccessRecorder_CacheEvict_err(t *testing.T) {
	lg := zap.NewNop()
	ar := &memoryAccessRecorder{
		records: make(map[string]*AccessRecord),
		logger:  lg,
	}
	err := ar.CacheEvict([]string{"key1", "key2"})
	assert.Equal(t, ErrMissingValue, err)
}

func TestMemoryAccessRecorder_LibriPut(t *testing.T) {
	lg := zap.NewNop()
	ar := &memoryAccessRecorder{
		records: make(map[string]*AccessRecord),
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
	assert.Equal(t, ErrMissingValue, err)
}

func TestGetNextEvictions(t *testing.T) {
	lg := server.NewDevLogger(zapcore.DebugLevel)
	now := time.Now()
	ar := &memoryAccessRecorder{
		params: &Parameters{
			RecentWindowDays:  1,
			EvictionBatchSize: 2,
			LRUCacheSize:      2,
		},
		records: map[string]*AccessRecord{
			// not evicted b/c put too recently
			"key1": {
				CachePutDateEarliest: now.Add(-2*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-time.Second),
				LibriPutOccurred:     true,
			},
			// not evicted b/c libri put hasn't occurred
			"key2": {
				CachePutDateEarliest: now.Add(-36*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-time.Second),
				LibriPutOccurred:     false,
			},
			// evictable & evicted in first batch
			"key3": {
				CachePutDateEarliest: now.Add(-50*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-6 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable & evicted in first batch
			"key4": {
				CachePutDateEarliest: now.Add(-50*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-5 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable & evicted in second batch
			"key5": {
				CachePutDateEarliest: now.Add(-50*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-4 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable but not evicted b/c of LRU cache
			"key6": {
				CachePutDateEarliest: now.Add(-50*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-3 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable but not evicted b/c of LRU cache
			"key7": {
				CachePutDateEarliest: now.Add(-50*time.Hour).Unix() / secsPerDay,
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

	ar.Evict(keys)
	keys, err = ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Equal(t, []string{"key5"}, keys)

	ar.Evict(keys)
	keys, err = ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Len(t, keys, 0)

}

func TestMemoryAccessRecorder_Evict_ok(t *testing.T) {
	lg := zap.NewNop()
	keys := []string{"key1", "key2"}
	ds := memoryAccessRecorder{
		records: map[string]*AccessRecord{
			"key1": newCachePutAccessRecord(),
			"key2": newCachePutAccessRecord(),
		},
		logger: lg,
	}
	err := ds.Evict(keys)
	assert.Nil(t, err)
	assert.Len(t, ds.records, 0)
}

func TestMemoryAccessRecorder_Evict_err(t *testing.T) {
	lg := zap.NewNop()
	keys := []string{"key1", "key2"}
	ds := memoryAccessRecorder{
		records: map[string]*AccessRecord{},
		logger:  lg,
	}
	err := ds.Evict(keys)
	assert.Equal(t, ErrMissingValue, err)
}
