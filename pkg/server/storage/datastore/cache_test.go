package datastore

import (
	"context"
	"encoding/hex"
	"errors"
	"math/rand"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/drausin/libri/libri/common/id"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/util"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

func TestCache_PutGet_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()
	valueSizes := []int{1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024}

	for _, valueSize := range valueSizes {
		accessRecorderDSClient := &fixedDatastoreClient{}
		ds := cache{
			params: storage.NewDefaultParameters(),
			client: &fixedDatastoreClient{},
			logger: lg,
			accessRecorder: &accessRecorder{
				params: storage.NewDefaultParameters(),
				client: accessRecorderDSClient,
				logger: lg,
			},
		}
		value1 := util.RandBytes(rng, valueSize)
		key := util.RandBytes(rng, id.Length)

		err := ds.Put(key, value1)
		assert.Nil(t, err)

		// put again just to see no-op
		err = ds.Put(key, value1)
		assert.Nil(t, err)

		// check this internal side effect b/c it is important for eviction
		accessLogValue1 := accessRecorderDSClient.value.(*storage.AccessRecord)
		assert.NotZero(t, accessLogValue1.CachePutTimeEarliest)
		assert.Zero(t, accessLogValue1.LibriPutTimeEarliest)
		assert.Zero(t, accessLogValue1.CacheGetTimeLatest)

		value2, err := ds.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value1, value2)

		// check side effect again
		accessLogValue2 := accessRecorderDSClient.value.(*storage.AccessRecord)
		assert.NotZero(t, accessLogValue2.CachePutTimeEarliest)
		assert.Equal(t, accessLogValue1.CachePutTimeEarliest, accessLogValue2.CachePutTimeEarliest)
		assert.Zero(t, accessLogValue2.LibriPutTimeEarliest)
		assert.NotZero(t, accessLogValue2.CacheGetTimeLatest)
	}
}

func TestCache_Put_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()

	// bad Key
	ds := &cache{
		params:         storage.NewDefaultParameters(),
		client:         &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{},
		logger:         lg,
	}
	err := ds.Put([]byte{1, 2, 3}, []byte{})
	assert.Equal(t, storage.ErrInvalidKeySize, err)

	// get error
	ds = &cache{
		params: storage.NewDefaultParameters(),
		client: &fixedDatastoreClient{
			getErr: errors.New("some get error"),
		},
		accessRecorder: &fixedAccessRecorder{},
		logger:         lg,
	}
	key := util.RandBytes(rng, id.Length)
	err = ds.Put(key, []byte{})
	assert.NotNil(t, err)

	// different values for same Key
	ds = &cache{
		params:         storage.NewDefaultParameters(),
		client:         &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{},
		logger:         lg,
	}
	err = ds.Put(key, []byte("value 1"))
	assert.Nil(t, err)
	err = ds.Put(key, []byte("value 2"))
	assert.Equal(t, storage.ErrExistingNotEqualNewValue, err)

	// value too large
	ds = &cache{
		params:         storage.NewDefaultParameters(),
		client:         &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{},
		logger:         lg,
	}
	err = ds.Put(key, util.RandBytes(rng, 3.5*1024*1024))
	assert.Equal(t, storage.ErrValueTooLarge, err)

	// doc put error
	ds = &cache{
		params: storage.NewDefaultParameters(),
		client: &fixedDatastoreClient{
			putErr: errors.New("some put error"),
		},
		accessRecorder: &fixedAccessRecorder{},
		logger:         lg,
	}
	err = ds.Put(key, util.RandBytes(rng, 1024))
	assert.NotNil(t, err)

	// access recorder error
	ds = &cache{
		params: storage.NewDefaultParameters(),
		client: &fixedDatastoreClient{},
		accessRecorder: &fixedAccessRecorder{
			cachePutErr: errors.New("some put error"),
		},
		logger: lg,
	}
	err = ds.Put(key, util.RandBytes(rng, 1024))
	assert.NotNil(t, err)
}

func TestCache_Get_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop()

	// missing doc error
	ds := &cache{
		params: storage.NewDefaultParameters(),
		client: &fixedDatastoreClient{
			getErr: datastore.ErrNoSuchEntity,
		},
		accessRecorder: &fixedAccessRecorder{},
		logger:         lg,
	}
	key := util.RandBytes(rng, id.Length)
	docBytes, err := ds.Get(key)
	assert.Equal(t, storage.ErrMissingValue, err)
	assert.Nil(t, docBytes)

	// other doc get error
	ds = &cache{
		params: storage.NewDefaultParameters(),
		client: &fixedDatastoreClient{
			getErr: errors.New("some get error"),
		},
		accessRecorder: &fixedAccessRecorder{},
		logger:         lg,
	}
	key = util.RandBytes(rng, id.Length)
	docBytes, err = ds.Get(key)
	assert.NotNil(t, err)
	assert.Nil(t, docBytes)

	// access recorder error
	ds = &cache{
		params: storage.NewDefaultParameters(),
		client: &fixedDatastoreClient{
			value: &MarshaledDocument{
				ValuePart1: []byte("some document"),
			},
		},
		accessRecorder: &fixedAccessRecorder{
			cacheGetErr: datastore.ErrNoSuchEntity,
		},
		logger: lg,
	}
	docBytes, err = ds.Get(key)
	assert.Equal(t, datastore.ErrNoSuchEntity, err)
	assert.Nil(t, docBytes)
}

func TestCache_EvictNext_ok(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{}
	ar := &fixedAccessRecorder{
		nextEvictions: [][]byte{}, // nothing to evict
	}
	dc := &cache{
		params:         storage.NewDefaultParameters(),
		client:         dsClient,
		accessRecorder: ar,
		logger:         lg,
	}
	err := dc.EvictNext()
	assert.Nil(t, err)

	evictionKeys := [][]byte{{1, 2, 3}, {4, 5, 6}}
	ar = &fixedAccessRecorder{
		nextEvictions: evictionKeys,
	}
	dc = &cache{
		params:         storage.NewDefaultParameters(),
		client:         dsClient,
		accessRecorder: ar,
		logger:         lg,
	}
	err = dc.EvictNext()
	assert.Nil(t, err)
	expectedDeleteKeys := []*datastore.Key{
		datastore.NameKey(documentKind, hex.EncodeToString([]byte{1, 2, 3}), nil),
		datastore.NameKey(documentKind, hex.EncodeToString([]byte{4, 5, 6}), nil),
	}
	assert.Equal(t, expectedDeleteKeys, dsClient.deleteKeys)
	assert.Equal(t, evictionKeys, ar.cacheEvictKeys)
}

func TestCache_EvictNext_err(t *testing.T) {
	lg := zap.NewNop()
	dsClient := &fixedDatastoreClient{}
	dc := &cache{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		accessRecorder: &fixedAccessRecorder{
			getEvictionBatchErr: errors.New("some getEvictionBatch error"),
		},
		logger: lg,
	}
	err := dc.EvictNext()
	assert.NotNil(t, err)

	dc = &cache{
		params: storage.NewDefaultParameters(),
		client: dsClient,
		accessRecorder: &fixedAccessRecorder{
			nextEvictions: [][]byte{{1, 2, 3}, {4, 5, 6}},
			cacheEvictErr: errors.New("some evict error"),
		},
		logger: lg,
	}
	err = dc.EvictNext()
	assert.NotNil(t, err)

	dc = &cache{
		params: storage.NewDefaultParameters(),
		client: &fixedDatastoreClient{
			deleteErr: errors.New("some delete error"),
		},
		accessRecorder: &fixedAccessRecorder{
			nextEvictions: [][]byte{{1, 2, 3}, {4, 5, 6}},
		},
		logger: lg,
	}
	err = dc.EvictNext()
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
	assert.Equal(t, storage.ErrValueTooLarge, err)
	assert.Nil(t, split)
}

type fixedDatastoreClient struct {
	value      interface{}
	getErr     error
	putErr     error
	deleteErr  error
	deleteKeys []*datastore.Key
	countValue int
	countErr   error
	runResult  *datastore.Iterator
}

func (f *fixedDatastoreClient) PutMulti(
	context.Context, []*datastore.Key, interface{},
) ([]*datastore.Key, error) {
	panic("implement me")
}

func (f *fixedDatastoreClient) GetMulti(
	ctx context.Context, keys []*datastore.Key, dst interface{},
) error {
	panic("implement me")
}

func (f *fixedDatastoreClient) Count(ctx context.Context, q *datastore.Query) (int, error) {
	return f.countValue, f.countErr
}

func (f *fixedDatastoreClient) Put(
	ctx context.Context, key *datastore.Key, value interface{},
) (*datastore.Key, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.value = value
	return key, nil
}

func (f *fixedDatastoreClient) Get(
	ctx context.Context, key *datastore.Key, dest interface{},
) error {
	if f.getErr != nil {
		return f.getErr
	}
	if f.value == nil {
		return datastore.ErrNoSuchEntity
	} else if key.Kind == accessRecordKind {
		dest.(*storage.AccessRecord).CacheGetTimeLatest =
			f.value.(*storage.AccessRecord).CacheGetTimeLatest
		dest.(*storage.AccessRecord).CachePutTimeEarliest =
			f.value.(*storage.AccessRecord).CachePutTimeEarliest
		dest.(*storage.AccessRecord).LibriPutTimeEarliest =
			f.value.(*storage.AccessRecord).LibriPutTimeEarliest
	} else if key.Kind == documentKind {
		dest.(*MarshaledDocument).ValuePart1 = f.value.(*MarshaledDocument).ValuePart1
		dest.(*MarshaledDocument).ValuePart2 = f.value.(*MarshaledDocument).ValuePart2
		dest.(*MarshaledDocument).ValuePart3 = f.value.(*MarshaledDocument).ValuePart3
	}
	return nil
}

func (f *fixedDatastoreClient) Delete(ctx context.Context, keys []*datastore.Key) error {
	f.value = nil
	f.deleteKeys = keys
	return f.deleteErr
}

func (f *fixedDatastoreClient) Run(ctx context.Context, q *datastore.Query) *datastore.Iterator {
	return f.runResult
}

type fixedDatastoreIterator struct {
	err    error
	keys   []*datastore.Key
	values []*storage.AccessRecord
	offset int
}

func (f *fixedDatastoreIterator) Init(iter *datastore.Iterator) {}

func (f *fixedDatastoreIterator) Next(dst interface{}) (*datastore.Key, error) {
	if f.err != nil {
		return nil, f.err
	}
	defer func() { f.offset++ }()
	if f.offset == len(f.values) {
		return nil, iterator.Done
	}
	dst.(*storage.AccessRecord).CacheGetTimeLatest = f.values[f.offset].CacheGetTimeLatest
	return f.keys[f.offset], nil
}

type fixedAccessRecorder struct {
	cachePutErr         error
	cacheGetErr         error
	cacheEvictErr       error
	cacheEvictKeys      [][]byte
	libriPutErr         error
	nextEvictions       [][]byte
	getEvictionBatchErr error
}

func (r *fixedAccessRecorder) CachePut(key []byte) error {
	return r.cachePutErr
}

func (r *fixedAccessRecorder) CacheGet(key []byte) error {
	return r.cacheGetErr
}

func (r *fixedAccessRecorder) CacheEvict(keys [][]byte) error {
	r.cacheEvictKeys = keys
	return r.cacheEvictErr
}

func (r *fixedAccessRecorder) LibriPut(key []byte) error {
	return r.libriPutErr
}

func (r *fixedAccessRecorder) GetNextEvictions() ([][]byte, error) {
	return r.nextEvictions, r.getEvictionBatchErr
}
