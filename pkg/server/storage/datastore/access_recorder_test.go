package datastore

import (
	"errors"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestAccessRecorder_CachePut_ok(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{}
	ds := &accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	err := ds.CachePut("some Key")
	assert.Nil(t, err)
	assert.NotZero(t, dsClient.value.(*storage.AccessRecord).CachePutDateEarliest)
	assert.NotZero(t, dsClient.value.(*storage.AccessRecord).CachePutTimeEarliest)
	assert.Zero(t, dsClient.value.(*storage.AccessRecord).LibriPutTimeEarliest)
	assert.Zero(t, dsClient.value.(*storage.AccessRecord).CacheGetTimeLatest)
	assert.False(t, dsClient.value.(*storage.AccessRecord).LibriPutOccurred)

	// second put should be no-op
	err = ds.CachePut("some Key")
	assert.Nil(t, err)
}

func TestAccessRecorder_CachePut_err(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{
		getErr: errors.New("some get error"),
	}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	err := ds.CachePut("some Key")
	assert.NotNil(t, err)

	dsClient = &fixedDatastoreClient{
		putErr: errors.New("some put error"),
	}
	ds = accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	err = ds.CachePut("some Key")
	assert.NotNil(t, err)
}

func TestAccessRecorder_CacheGet(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{
		value: &storage.AccessRecord{CachePutTimeEarliest: time.Now()},
	}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	err := ds.CacheGet("some Key")
	assert.Nil(t, err)
	assert.NotZero(t, dsClient.value.(*storage.AccessRecord).CachePutTimeEarliest)
	assert.Zero(t, dsClient.value.(*storage.AccessRecord).LibriPutTimeEarliest)
	assert.NotZero(t, dsClient.value.(*storage.AccessRecord).CacheGetTimeLatest)
}

func TestAccessRecorder_LibriPut(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{
		value: &storage.AccessRecord{CachePutTimeEarliest: time.Now()},
	}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	err := ds.LibriPut("some Key")
	assert.Nil(t, err)
	assert.NotZero(t, dsClient.value.(*storage.AccessRecord).CachePutTimeEarliest)
	assert.NotZero(t, dsClient.value.(*storage.AccessRecord).LibriPutTimeEarliest)
	assert.True(t, dsClient.value.(*storage.AccessRecord).LibriPutOccurred)
	assert.Zero(t, dsClient.value.(*storage.AccessRecord).CacheGetTimeLatest)
}

func TestAccessRecorder_CacheEvict_ok(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	keyNames := []string{"key1", "key2"}
	err := ds.CacheEvict(keyNames)
	assert.Nil(t, err)
	expectedDeleteKeys := []*datastore.Key{
		datastore.NameKey(accessRecordKind, "key1", nil),
		datastore.NameKey(accessRecordKind, "key2", nil),
	}
	assert.Equal(t, expectedDeleteKeys, dsClient.deleteKeys)
}

func TestAccessRecorder_CacheEvict_err(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{
		deleteErr: errors.New("some delete error"),
	}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	keyNames := []string{"key1", "key2"}
	err := ds.CacheEvict(keyNames)
	assert.NotNil(t, err)
}

func TestAccessRecorder_GetNextEvictions_ok(t *testing.T) {
	lg := zap.NewNop()
	params := &storage.Parameters{
		RecentWindowDays:  1,
		LRUCacheSize:      2,
		EvictionBatchSize: 2,
	}

	dsKeys := []*datastore.Key{
		datastore.NameKey(accessRecordKind, "key1", nil),
		datastore.NameKey(accessRecordKind, "key2", nil),
		datastore.NameKey(accessRecordKind, "key3", nil),
	}

	// should have eviction when count value > LRU cache size
	dsClient := &fixedDatastoreClient{
		countValue: 8,
	}
	now := time.Now()
	ds := accessRecorder{
		client: dsClient,
		iter: &fixedDatastoreIterator{
			keys: dsKeys,
			values: []*storage.AccessRecord{
				{CacheGetTimeLatest: now.Add(1 * time.Second)},
				{CacheGetTimeLatest: now.Add(2 * time.Second)},
				{CacheGetTimeLatest: now.Add(3 * time.Second)},
			},
		},
		params: params,
		logger: lg,
	}
	expected := []string{"key1", "key2"}
	keys, err := ds.GetNextEvictions()
	sort.Strings(keys)
	assert.Nil(t, err)
	assert.Equal(t, expected, keys)

	// should not have any evictions when count value <= LRU cache size
	dsClient = &fixedDatastoreClient{
		runResult:  &datastore.Iterator{},
		countValue: 2,
	}
	ds = accessRecorder{
		client: dsClient,
		params: params,
		logger: lg,
	}
	keys, err = ds.GetNextEvictions()
	assert.Nil(t, err)
	assert.Len(t, keys, 0)
}

func TestAccessRecorder_GetNextEvictions_err(t *testing.T) {
	lg := zap.NewNop()
	params := &storage.Parameters{
		RecentWindowDays:  1,
		LRUCacheSize:      2,
		EvictionBatchSize: 3,
	}

	// check count error bubbles up
	dsClient := &fixedDatastoreClient{
		countErr: errors.New("some count error"),
	}
	ds := accessRecorder{
		client: dsClient,
		params: params,
		logger: lg,
	}
	keys, err := ds.GetNextEvictions()
	assert.NotNil(t, err)
	assert.Nil(t, keys)

	// check queryAllKeys error bubbles up
	dsClient = &fixedDatastoreClient{
		countValue: 4,
	}
	ds = accessRecorder{
		client: dsClient,
		iter: &fixedDatastoreIterator{
			err: errors.New("some iter error"),
		},
		params: params,
		logger: lg,
	}
	keys, err = ds.GetNextEvictions()
	assert.NotNil(t, err)
	assert.Nil(t, keys)
}

func TestAccessRecorder_Evict_ok(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	keys := []string{"key1", "key2"}
	err := ds.CacheEvict(keys)
	assert.Nil(t, err)

	expected := []*datastore.Key{
		datastore.NameKey(accessRecordKind, "key1", nil),
		datastore.NameKey(accessRecordKind, "key2", nil),
	}
	assert.Equal(t, expected, dsClient.deleteKeys)
}

func TestAccessRecorder_Evict_err(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{
		deleteErr: errors.New("some delete error"),
	}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	keys := []string{"key1", "key2"}
	err := ds.CacheEvict(keys)
	assert.NotNil(t, err)
}

func TestAccessRecorder_update_err(t *testing.T) {
	lg := zap.NewNop()
	// no log for given Key
	dsClient := &fixedDatastoreClient{
		getErr: datastore.ErrNoSuchEntity,
	}
	ds := accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
	}
	err := ds.LibriPut("some Key without log")
	assert.Equal(t, datastore.ErrNoSuchEntity, err)

	// put error
	dsClient = &fixedDatastoreClient{
		value:  &storage.AccessRecord{CachePutTimeEarliest: time.Now()},
		putErr: errors.New("some put error"),
	}
	ds = accessRecorder{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		logger: lg,
	}
	err = ds.LibriPut("some Key")
	assert.NotNil(t, err)
}
