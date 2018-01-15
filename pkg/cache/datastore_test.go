package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/drausin/libri/libri/common/id"
	"github.com/elxirhealth/courier/pkg/base/util"
	"github.com/stretchr/testify/assert"
)

func TestDatastoreCache_PutGet_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	valueSizes := []int{1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024}

	for _, valueSize := range valueSizes {
		accessRecorderDSClient := &fixedDatastoreClient{}
		ds := datastoreCache{
			client: &fixedDatastoreClient{},
			accessRecorder: &datastoreAccessRecorder{
				client: accessRecorderDSClient,
			},
		}
		value1 := util.RandBytes(rng, valueSize)
		key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))

		err := ds.Put(key, value1)
		assert.Nil(t, err)

		// put again just to see no-op
		err = ds.Put(key, value1)
		assert.Nil(t, err)

		// check this internal side effect b/c it is important for eviction
		accessLogValue1 := accessRecorderDSClient.value.(*AccessRecord)
		assert.NotZero(t, accessLogValue1.CachePutTimeEarliest)
		assert.Zero(t, accessLogValue1.LibriPutTimeEarliest)
		assert.Zero(t, accessLogValue1.CacheGetTimeLatest)

		value2, err := ds.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value1, value2)

		// check side effect again
		accessLogValue2 := accessRecorderDSClient.value.(*AccessRecord)
		assert.NotZero(t, accessLogValue2.CachePutTimeEarliest)
		assert.Equal(t, accessLogValue1.CachePutTimeEarliest, accessLogValue2.CachePutTimeEarliest)
		assert.Zero(t, accessLogValue2.LibriPutTimeEarliest)
		assert.NotZero(t, accessLogValue2.CacheGetTimeLatest)
	}
}

func TestDatastoreCache_Put_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))

	// bad key
	ds := &datastoreCache{
		client:         &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{},
	}
	err := ds.Put("too short key", []byte{})
	assert.Equal(t, ErrInvalidKeySize, err)

	// get error
	ds = &datastoreCache{
		client: &fixedDatastoreClient{
			getErr: errors.New("some get error"),
		},
		accessRecorder: &fixedAccessRecorder{},
	}
	key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))
	err = ds.Put(key, []byte{})
	assert.NotNil(t, err)

	// different values for same key
	ds = &datastoreCache{
		client:         &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{},
	}
	err = ds.Put(key, []byte("value 1"))
	assert.Nil(t, err)
	err = ds.Put(key, []byte("value 2"))
	assert.Equal(t, ErrExistingNotEqualNewValue, err)

	// value too large
	ds = &datastoreCache{
		client:         &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{},
	}
	err = ds.Put(key, util.RandBytes(rng, 3.5*1024*1024))
	assert.Equal(t, ErrValueTooLarge, err)

	// doc put error
	ds = &datastoreCache{
		client: &fixedDatastoreClient{
			putErr: errors.New("some put error"),
		},
		accessRecorder: &fixedAccessRecorder{},
	}
	err = ds.Put(key, util.RandBytes(rng, 1024))
	assert.NotNil(t, err)

	// access recorder error
	ds = &datastoreCache{
		client: &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{
			cachePutErr: errors.New("some put error"),
		},
	}
	err = ds.Put(key, util.RandBytes(rng, 1024))
	assert.NotNil(t, err)
}

func TestDatastoreCache_Get_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))

	// missing doc error
	ds := &datastoreCache{
		client: &fixedDatastoreClient{
			getErr: datastore.ErrNoSuchEntity,
		},
		accessRecorder: &fixedAccessRecorder{},
	}
	key := fmt.Sprintf("%x", util.RandBytes(rng, id.Length))
	docBytes, err := ds.Get(key)
	assert.Equal(t, ErrMissingValue, err)
	assert.Nil(t, docBytes)

	// other doc get error
	ds = &datastoreCache{
		client: &fixedDatastoreClient{
			getErr: errors.New("some get error"),
		},
		accessRecorder: &fixedAccessRecorder{},
	}
	key = fmt.Sprintf("%x", util.RandBytes(rng, id.Length))
	docBytes, err = ds.Get(key)
	assert.NotNil(t, err)
	assert.Nil(t, docBytes)

	// access recorder error
	ds = &datastoreCache{
		client: &fixedDatastoreClient{
			value: &MarshaledDocument{
				ValuePart1: []byte("some document"),
			},
		},
		accessRecorder: &fixedAccessRecorder{
			cacheGetErr: datastore.ErrNoSuchEntity,
		},
	}
	docBytes, err = ds.Get(key)
	assert.Equal(t, datastore.ErrNoSuchEntity, err)
	assert.Nil(t, docBytes)
}

func TestDatastoreCache_EvictNext_ok(t *testing.T) {
	dsClient := &fixedDatastoreClient{}
	evictionKeys := []string{"key1", "key2"}
	dc := &datastoreCache{
		client: dsClient,
		accessRecorder: &fixedAccessRecorder{
			nextEvictions: evictionKeys,
		},
	}
	err := dc.EvictNext()
	assert.Nil(t, err)
	expectedDeleteKeys := []*datastore.Key{
		datastore.NameKey(documentKind, "key1", nil),
		datastore.NameKey(documentKind, "key2", nil),
	}
	assert.Equal(t, expectedDeleteKeys, dsClient.deleteKeys)
}

func TestDatastoreCache_EvictNext_err(t *testing.T) {
	dsClient := &fixedDatastoreClient{}
	dc := &datastoreCache{
		client: dsClient,
		accessRecorder: &fixedAccessRecorder{
			getEvictionBatchErr: errors.New("some getEvictionBatch error"),
		},
	}
	err := dc.EvictNext()
	assert.NotNil(t, err)
}

func TestDatastoreAccessRecorder_CachePut_ok(t *testing.T) {
	dsClient := &fixedDatastoreClient{}
	ds := &datastoreAccessRecorder{client: dsClient}
	err := ds.CachePut("some key")
	assert.Nil(t, err)
	assert.NotZero(t, dsClient.value.(*AccessRecord).CachePutDateEarliest)
	assert.NotZero(t, dsClient.value.(*AccessRecord).CachePutTimeEarliest)
	assert.Zero(t, dsClient.value.(*AccessRecord).LibriPutTimeEarliest)
	assert.Zero(t, dsClient.value.(*AccessRecord).CacheGetTimeLatest)
	assert.False(t, dsClient.value.(*AccessRecord).LibriPutOccurred)

	// second put should be no-op
	err = ds.CachePut("some key")
	assert.Nil(t, err)
}

func TestDatastoreAccessRecorder_CachePut_err(t *testing.T) {
	dsClient := &fixedDatastoreClient{
		getErr: errors.New("some get error"),
	}
	ds := datastoreAccessRecorder{client: dsClient}
	err := ds.CachePut("some key")
	assert.NotNil(t, err)

	dsClient = &fixedDatastoreClient{
		putErr: errors.New("some put error"),
	}
	ds = datastoreAccessRecorder{client: dsClient}
	err = ds.CachePut("some key")
	assert.NotNil(t, err)
}

func TestDatastoreAccessRecorder_CacheGet(t *testing.T) {
	dsClient := &fixedDatastoreClient{
		value: &AccessRecord{CachePutTimeEarliest: time.Now()},
	}
	ds := datastoreAccessRecorder{client: dsClient}
	err := ds.CacheGet("some key")
	assert.Nil(t, err)
	assert.NotZero(t, dsClient.value.(*AccessRecord).CachePutTimeEarliest)
	assert.Zero(t, dsClient.value.(*AccessRecord).LibriPutTimeEarliest)
	assert.NotZero(t, dsClient.value.(*AccessRecord).CacheGetTimeLatest)
}

func TestDatastoreAccessRecorder_LibriPut(t *testing.T) {
	dsClient := &fixedDatastoreClient{
		value: &AccessRecord{CachePutTimeEarliest: time.Now()},
	}
	ds := datastoreAccessRecorder{client: dsClient}
	err := ds.LibriPut("some key")
	assert.Nil(t, err)
	assert.NotZero(t, dsClient.value.(*AccessRecord).CachePutTimeEarliest)
	assert.NotZero(t, dsClient.value.(*AccessRecord).LibriPutTimeEarliest)
	assert.True(t, dsClient.value.(*AccessRecord).LibriPutOccurred)
	assert.Zero(t, dsClient.value.(*AccessRecord).CacheGetTimeLatest)
}

func TestDatastoreAccessRecorder_CacheEvict(t *testing.T) {
	dsClient := &fixedDatastoreClient{}
	ds := datastoreAccessRecorder{client: dsClient}
	keyNames := []string{"key1", "key2"}
	err := ds.CacheEvict(keyNames)
	assert.Nil(t, err)
	expectedDeleteKeys := []*datastore.Key{
		datastore.NameKey(accessRecordKind, "key1", nil),
		datastore.NameKey(accessRecordKind, "key2", nil),
	}
	assert.Equal(t, expectedDeleteKeys, dsClient.deleteKeys)
}

func TestDatastoreAccessRecorder_GetNextEvictions_ok(t *testing.T) {
	dsKeys := []*datastore.Key{
		datastore.NameKey(accessRecordKind, "key1", nil),
		datastore.NameKey(accessRecordKind, "key2", nil),
	}
	dsClient := &fixedDatastoreClient{
		queryAllKeysResult: dsKeys,
	}
	ds := datastoreAccessRecorder{
		client: dsClient,
		params: &Parameters{RecentWindow: 24 * time.Hour},
	}
	expected := []string{"key1", "key2"}
	keys, err := ds.GetNextEvictions()
	assert.Nil(t, err)
	assert.Equal(t, expected, keys)
}

func TestDatastoreAccessRecorder_GetNextEvictions_err(t *testing.T) {
	dsClient := &fixedDatastoreClient{
		queryAllKeysErr: errors.New("some queryAllKeys error"),
	}
	ds := datastoreAccessRecorder{
		client: dsClient,
		params: &Parameters{RecentWindow: 24 * time.Hour},
	}
	keys, err := ds.GetNextEvictions()
	assert.NotNil(t, err)
	assert.Nil(t, keys)
}

func TestDatastoreAccessRecorder_update_err(t *testing.T) {
	// no log for given key
	dsClient := &fixedDatastoreClient{
		getErr: datastore.ErrNoSuchEntity,
	}
	ds := datastoreAccessRecorder{client: dsClient}
	err := ds.LibriPut("some key without log")
	assert.Equal(t, datastore.ErrNoSuchEntity, err)

	// put error
	dsClient = &fixedDatastoreClient{
		value:  &AccessRecord{CachePutTimeEarliest: time.Now()},
		putErr: errors.New("some put error"),
	}
	ds = datastoreAccessRecorder{client: dsClient}
	err = ds.LibriPut("some key")
	assert.NotNil(t, err)
}

func TestSplitJoinValue(t *testing.T) {
	rng := rand.New(rand.NewSource(0))

	value1 := util.RandBytes(rng, maxValuePartSize/2)
	split, err := splitValue(value1)
	assert.Nil(t, err)
	assert.Len(t, split.ValuePart1, len(value1))
	assert.Nil(t, split.ValuePart2)
	assert.Nil(t, split.ValuePart3)
	value2 := joinValue(split)
	assert.Equal(t, value1, value2)

	value1 = util.RandBytes(rng, maxValuePartSize*1.5)
	split, err = splitValue(value1)
	assert.Nil(t, err)
	assert.Len(t, split.ValuePart1, maxValuePartSize)
	assert.Len(t, split.ValuePart2, maxValuePartSize/2)
	assert.Nil(t, split.ValuePart3)
	value2 = joinValue(split)
	assert.Equal(t, value1, value2)

	value1 = util.RandBytes(rng, maxValuePartSize*2.5)
	split, err = splitValue(value1)
	assert.Nil(t, err)
	assert.Len(t, split.ValuePart1, maxValuePartSize)
	assert.Len(t, split.ValuePart2, maxValuePartSize)
	assert.Len(t, split.ValuePart3, maxValuePartSize/2)
	value2 = joinValue(split)
	assert.Equal(t, value1, value2)

	value1 = util.RandBytes(rng, maxValuePartSize*3.5)
	split, err = splitValue(value1)
	assert.Equal(t, ErrValueTooLarge, err)
	assert.Nil(t, split)
}

type fixedDatastoreClient struct {
	value              interface{}
	getErr             error
	putErr             error
	deleteErr          error
	deleteKeys         []*datastore.Key
	queryAllKeysResult []*datastore.Key
	queryAllKeysErr    error
}

func (f *fixedDatastoreClient) put(key *datastore.Key, value interface{}) (*datastore.Key, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.value = value
	return key, nil
}

func (f *fixedDatastoreClient) get(key *datastore.Key, dest interface{}) error {
	if f.getErr != nil {
		return f.getErr
	}
	if f.value == nil {
		return datastore.ErrNoSuchEntity
	} else if key.Kind == accessRecordKind {
		dest.(*AccessRecord).CacheGetTimeLatest = f.value.(*AccessRecord).CacheGetTimeLatest
		dest.(*AccessRecord).CachePutTimeEarliest =
			f.value.(*AccessRecord).CachePutTimeEarliest
		dest.(*AccessRecord).LibriPutTimeEarliest =
			f.value.(*AccessRecord).LibriPutTimeEarliest
	} else if key.Kind == documentKind {
		dest.(*MarshaledDocument).ValuePart1 = f.value.(*MarshaledDocument).ValuePart1
		dest.(*MarshaledDocument).ValuePart2 = f.value.(*MarshaledDocument).ValuePart2
		dest.(*MarshaledDocument).ValuePart3 = f.value.(*MarshaledDocument).ValuePart3
	}
	return nil
}

func (f *fixedDatastoreClient) delete(keys []*datastore.Key) error {
	f.value = nil
	f.deleteKeys = keys
	return f.deleteErr
}

func (f *fixedDatastoreClient) queryAllKeys(
	ctx context.Context, q *datastore.Query,
) ([]*datastore.Key, error) {
	return f.queryAllKeysResult, f.queryAllKeysErr
}

type fixedAccessRecorder struct {
	cachePutErr         error
	cacheGetErr         error
	cacheEvict          error
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
	return r.cacheEvict
}

func (r *fixedAccessRecorder) LibriPut(key string) error {
	return r.libriPutErr
}

func (r *fixedAccessRecorder) GetNextEvictions() ([]string, error) {
	return r.nextEvictions, r.getEvictionBatchErr
}
