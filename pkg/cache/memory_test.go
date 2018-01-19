package cache

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/id"
	"github.com/elxirhealth/courier/pkg/base/util"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestMemoryCache_PutGet_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	params := NewDefaultParameters()
	c, ar := NewMemory(params)
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

	// bad key
	c, _ := NewMemory(NewDefaultParameters())
	err := c.Put("too short key", []byte{})
	assert.Equal(t, ErrInvalidKeySize, err)

	// different values for same key
	c, _ = NewMemory(NewDefaultParameters())
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
	}
	err = c.Put(key, []byte("value 1"))
	assert.NotNil(t, err)
}

func TestMemoryCache_Get_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	c, _ := NewMemory(NewDefaultParameters())
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
	}
	err = c.Put(key, []byte("value 1"))
	assert.Nil(t, err)
	value, err = c.Get(key)
	assert.NotNil(t, err)
	assert.Nil(t, value)
}

func TestMemoryCache_EvictNext_ok(t *testing.T) {
	evictionKeys := []string{"key1", "key2"}
	ar := &fixedAccessRecorder{
		nextEvictions: evictionKeys,
	}
	c := &memoryCache{
		accessRecorder: ar,
		docs: map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
		},
	}
	err := c.EvictNext()
	assert.Nil(t, err)

	assert.Len(t, c.docs, 1)
	assert.Equal(t, evictionKeys, ar.evictKeys)
}

func TestMemoryCache_EvictNext_err(t *testing.T) {
	// check GetNextEvictions error bubbles up
	ar := &fixedAccessRecorder{
		getEvictionBatchErr: errors.New("some error"),
	}
	c := &memoryCache{
		accessRecorder: ar,
	}
	err := c.EvictNext()
	assert.NotNil(t, err)

	// check accessRecorder.Evict error bubbes up
	ar = &fixedAccessRecorder{
		evictErr: errors.New("some evict error"),
	}
	c = &memoryCache{
		accessRecorder: ar,
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
	}
	err = c.EvictNext()
	assert.NotNil(t, err)
}

func TestMemoryAccessRecorder_CachePut(t *testing.T) {
	ar := &memoryAccessRecorder{records: make(map[string]*AccessRecord)}
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
	ar := &memoryAccessRecorder{records: make(map[string]*AccessRecord)}
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

func TestMemoryAccessRecorder_CacheEvict(t *testing.T) {
	ar := &memoryAccessRecorder{records: make(map[string]*AccessRecord)}
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

func TestMemoryAccessRecorder_LibriPut(t *testing.T) {
	ar := &memoryAccessRecorder{records: make(map[string]*AccessRecord)}
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
	now := time.Now()
	ar := &memoryAccessRecorder{
		params: &Parameters{
			RecentWindow:      10 * time.Minute,
			EvictionBatchSize: 2,
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
			// evicted in first batch
			"key3": {
				CachePutDateEarliest: now.Add(-36*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-3 * time.Second),
				LibriPutOccurred:     true,
			},
			// evicted in first batch
			"key4": {
				CachePutDateEarliest: now.Add(-40*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-2 * time.Second),
				LibriPutOccurred:     true,
			},
			// evicted in second batch
			"key5": {
				CachePutDateEarliest: now.Add(-36*time.Hour).Unix() / secsPerDay,
				CacheGetTimeLatest:   now.Add(-1 * time.Second),
				LibriPutOccurred:     true,
			},
		},
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

func TestKeyGetTimes(t *testing.T) {
	now := time.Now()
	x := &keyGetTimes{
		{key: "b", getTime: now.Add(2 * time.Second)},
		{key: "a", getTime: now.Add(1 * time.Second)},
		{key: "c", getTime: now.Add(3 * time.Second)},
	}
	heap.Init(x)
	heap.Push(x, keyGetTime{key: "d", getTime: now.Add(4 * time.Second)})
	assert.Equal(t, 4, x.Len())

	x1 := heap.Pop(x).(keyGetTime)
	x2 := heap.Pop(x).(keyGetTime)
	x3 := heap.Pop(x).(keyGetTime)
	x4 := heap.Pop(x).(keyGetTime)

	assert.Equal(t, "d", x1.key)
	assert.Equal(t, "c", x2.key)
	assert.Equal(t, "b", x3.key)
	assert.Equal(t, "a", x4.key)

	assert.True(t, x1.getTime.After(x2.getTime))
	assert.True(t, x2.getTime.After(x3.getTime))
	assert.True(t, x3.getTime.After(x4.getTime))
}
