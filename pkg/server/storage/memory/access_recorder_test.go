package memory

import (
	"sort"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/logging"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestAccessRecorder_CachePut(t *testing.T) {
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

func TestAccessRecorder_CacheGet(t *testing.T) {
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

func TestAccessRecorder_CacheEvict_ok(t *testing.T) {
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

func TestAccessRecorder_CacheEvict_err(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	err := ar.CacheEvict([]string{"key1", "key2"})
	assert.Equal(t, storage.ErrMissingValue, err)
}

func TestAccessRecorder_LibriPut(t *testing.T) {
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
