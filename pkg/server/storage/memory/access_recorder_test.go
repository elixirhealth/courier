package memory

import (
	"bytes"
	"encoding/hex"
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
	key := []byte{1, 2, 3}
	keyHex := hex.EncodeToString(key)
	err := ar.CachePut(key)
	assert.Nil(t, err)
	assert.NotZero(t, ar.records[keyHex].CachePutDateEarliest)
	assert.NotZero(t, ar.records[keyHex].CachePutTimeEarliest)
	assert.Zero(t, ar.records[keyHex].LibriPutTimeEarliest)
	assert.Zero(t, ar.records[keyHex].CacheGetTimeLatest)
	assert.False(t, ar.records[keyHex].LibriPutOccurred)

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
	key := []byte{1, 2, 3}
	keyHex := hex.EncodeToString(key)
	err := ar.CachePut(key)
	assert.Nil(t, err)

	err = ar.CacheGet(key)
	assert.Nil(t, err)
	assert.NotZero(t, ar.records[keyHex].CachePutTimeEarliest)
	assert.Zero(t, ar.records[keyHex].LibriPutTimeEarliest)
	assert.NotZero(t, ar.records[keyHex].CacheGetTimeLatest)

	err = ar.CacheGet([]byte{4, 5, 6})
	assert.Equal(t, storage.ErrMissingValue, err)
}

func TestAccessRecorder_CacheEvict_ok(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	keys := [][]byte{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}
	for _, key := range keys {
		err := ar.CachePut(key)
		assert.Nil(t, err)
	}

	err := ar.CacheEvict([][]byte{{1, 2, 3}, {4, 5, 6}})
	assert.Nil(t, err)

	err = ar.CacheGet([]byte{1, 2, 3})
	assert.Equal(t, storage.ErrMissingValue, err)
	err = ar.CacheGet([]byte{4, 5, 6})
	assert.Equal(t, storage.ErrMissingValue, err)
	err = ar.CacheGet([]byte{7, 8, 9})
	assert.Nil(t, err)
}

func TestAccessRecorder_CacheEvict_err(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	err := ar.CacheEvict([][]byte{{1, 2, 3}, {4, 5, 6}})
	assert.Equal(t, storage.ErrMissingValue, err)
}

func TestAccessRecorder_LibriPut(t *testing.T) {
	lg := zap.NewNop()
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		logger:  lg,
	}
	key := []byte{1, 2, 3}
	keyHex := hex.EncodeToString(key)
	err := ar.CachePut(key)
	assert.Nil(t, err)

	err = ar.LibriPut(key)
	assert.Nil(t, err)
	assert.NotZero(t, ar.records[keyHex].CachePutTimeEarliest)
	assert.NotZero(t, ar.records[keyHex].LibriPutTimeEarliest)
	assert.True(t, ar.records[keyHex].LibriPutOccurred)
	assert.Zero(t, ar.records[keyHex].CacheGetTimeLatest)

	err = ar.LibriPut([]byte{4, 5, 6})
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
			hex.EncodeToString([]byte{1}): {
				CachePutDateEarliest: timeToDate(now.Add(-2 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-time.Second),
				LibriPutOccurred:     true,
			},
			// not evicted b/c libri put hasn't occurred
			hex.EncodeToString([]byte{2}): {
				CachePutDateEarliest: timeToDate(now.Add(-36 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-time.Second),
				LibriPutOccurred:     false,
			},
			// evictable & evicted in first batch
			hex.EncodeToString([]byte{3}): {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-6 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable & evicted in first batch
			hex.EncodeToString([]byte{4}): {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-5 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable & evicted in second batch
			hex.EncodeToString([]byte{5}): {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-4 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable but not evicted b/c of LRU cache
			hex.EncodeToString([]byte{6}): {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-3 * time.Second),
				LibriPutOccurred:     true,
			},
			// evictable but not evicted b/c of LRU cache
			hex.EncodeToString([]byte{7}): {
				CachePutDateEarliest: timeToDate(now.Add(-50 * time.Hour)),
				CacheGetTimeLatest:   now.Add(-2 * time.Second),
				LibriPutOccurred:     true,
			},
		},
		logger: lg,
	}

	keys, err := ar.GetNextEvictions()
	assert.Nil(t, err)
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	assert.Equal(t, [][]byte{{3}, {4}}, keys)

	ar.CacheEvict(keys)
	keys, err = ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Equal(t, [][]byte{{5}}, keys)

	ar.CacheEvict(keys)
	keys, err = ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Len(t, keys, 0)

}
