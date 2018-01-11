package server

import (
	"context"
	"math/rand"
	"net"
	"testing"

	"github.com/drausin/libri/libri/common/id"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elxirhealth/courier/pkg/cache"
	api "github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestNewCourier_ok(t *testing.T) {
	config := NewDefaultConfig()
	c, err := NewCourier(config)
	assert.Nil(t, err)
	assert.NotNil(t, c.(*courier).clientID)
	// assert.NotNil(t, c.(*courier).cache)  // TODO (drausin) add when have in-mem cache
	assert.NotNil(t, c.(*courier).getter)
	assert.NotNil(t, c.(*courier).acquirer)
	assert.NotNil(t, c.(*courier).libriPutQueue)
	assert.Equal(t, config, c.(*courier).config)
}

func TestNewCourier_err(t *testing.T) {
	badConfigs := map[string]*Config{
		"missing clientID file": NewDefaultConfig().WithClientIDFilepath("missing.der"),
		"emptyProjectID":        NewDefaultConfig().WithCacheStorage(DataStore),
		"empty librarian addrs": NewDefaultConfig().WithLibrarianAddrs([]*net.TCPAddr{}),
	}
	for desc, badConfig := range badConfigs {
		c, err := NewCourier(badConfig)
		assert.NotNil(t, err, desc)
		assert.Nil(t, c)
	}
}

func TestCourier_Put_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	value, key := libriapi.NewTestDocument(rng)
	valueBytes, err := proto.Marshal(value)
	assert.Nil(t, err)
	rq := &api.PutRequest{
		Key:   key.Bytes(),
		Value: value,
	}

	// when cache has value, Put request should leave existing
	cc := &fixedCache{value: valueBytes}
	c := &courier{
		cache:         cc,
		libriPutQueue: make(chan string, 1),
	}
	rp, err := c.Put(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, api.PutOperation_LEFT_EXISTING, rp.Operation)
	assert.Equal(t, key.String(), cc.getKey)

	// when cache doesn't have value, Put request should store in cache and add
	// to libriPutQueue queue
	cc = &fixedCache{getErr: cache.ErrMissingValue}
	c = &courier{
		cache:         cc,
		libriPutQueue: make(chan string, 1),
	}
	rp, err = c.Put(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, api.PutOperation_STORED, rp.Operation)
	assert.Equal(t, key.String(), cc.getKey)
	assert.Equal(t, key.String(), cc.putKey)
	assert.Equal(t, valueBytes, cc.value)
	toPutKey := <-c.libriPutQueue
	assert.Equal(t, key.String(), toPutKey)
}

func TestCourier_Put_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	value, key := libriapi.NewTestDocument(rng)
	okRq := &api.PutRequest{
		Key:   key.Bytes(),
		Value: value,
	}

	cases := map[string]struct {
		rq  *api.PutRequest
		c   *courier
		err error
	}{
		"bad request": {
			rq: &api.PutRequest{},
			c:  &courier{},
		},
		"cache Get error": {
			rq: okRq,
			c: &courier{
				cache: &fixedCache{getErr: errors.New("some Get error")},
			},
		},
		"existing not equal new doc": {
			rq: okRq,
			c: &courier{
				cache: &fixedCache{value: []byte{1, 2, 3, 4}},
			},
			err: ErrExistingNotEqualNewDocument,
		},
		"cache Put error": {
			rq: okRq,
			c: &courier{
				cache: &fixedCache{
					getErr: cache.ErrMissingValue,
					putErr: errors.New("some Put error"),
				},
			},
		},
		"full Libri put queue": {
			rq: okRq,
			c: &courier{
				cache: &fixedCache{
					getErr: cache.ErrMissingValue,
				},
				libriPutQueue: make(chan string), // no slack
			},
			err: ErrFullLibriPutQueue,
		},
	}

	for desc, c := range cases {
		rp, err := c.c.Put(context.Background(), c.rq)
		assert.Nil(t, rp, desc)
		if c.err != nil {
			// specific error
			assert.Equal(t, c.err, err, desc)
		} else {
			// non-nil error
			assert.NotNil(t, err, desc)
		}
	}
}

func TestCourier_Get_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	value, key := libriapi.NewTestDocument(rng)
	valueBytes, err := proto.Marshal(value)
	assert.Nil(t, err)
	rq := &api.GetRequest{
		Key: key.Bytes(),
	}

	// when cache has doc, Get should return it
	cc := &fixedCache{value: valueBytes}
	c := &courier{cache: cc}
	rp, err := c.Get(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, value, rp.Value)
	assert.Equal(t, key.String(), cc.getKey)

	// when cache doesn't have doc but libri does, Get should return it
	cc = &fixedCache{getErr: cache.ErrMissingValue}
	acq := &fixedAcquirer{doc: value}
	c = &courier{
		cache:    cc,
		acquirer: acq,
	}
	rp, err = c.Get(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, value, rp.Value)
	assert.Equal(t, key.String(), cc.getKey)
	assert.Equal(t, key.String(), cc.putKey)
	assert.Equal(t, valueBytes, cc.value)
	assert.Equal(t, key, acq.docKey)
}

func TestCourier_Get_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	value, key := libriapi.NewTestDocument(rng)
	okRq := &api.GetRequest{
		Key: key.Bytes(),
	}
	cases := map[string]struct {
		rq  *api.GetRequest
		c   *courier
		err error
	}{
		"bad request": {
			rq: &api.GetRequest{},
			c:  &courier{},
		},
		"cache Get error": {
			rq: okRq,
			c: &courier{
				cache: &fixedCache{getErr: errors.New("some Get error")},
			},
		},
		"bad marshaled doc": {
			rq: okRq,
			c: &courier{
				cache: &fixedCache{value: []byte{1, 2, 3, 4}},
			},
		},
		"acquirer Acquire error": {
			rq: okRq,
			c: &courier{
				cache:    &fixedCache{getErr: cache.ErrMissingValue},
				acquirer: &fixedAcquirer{err: errors.New("some Acquire error")},
			},
		},
		"acquirer Acquire missing doc": {
			rq: okRq,
			c: &courier{
				cache:    &fixedCache{getErr: cache.ErrMissingValue},
				acquirer: &fixedAcquirer{err: libriapi.ErrMissingDocument},
			},
		},
		"cache Put error": {
			rq: okRq,
			c: &courier{
				cache: &fixedCache{
					getErr: cache.ErrMissingValue,
					putErr: errors.New("some Put error"),
				},
				acquirer: &fixedAcquirer{doc: value},
			},
		},
	}

	for desc, c := range cases {
		rp, err := c.c.Get(context.Background(), c.rq)
		assert.Nil(t, rp, desc)
		if c.err != nil {
			// specific error
			assert.Equal(t, c.err, err, desc)
		} else {
			// non-nil error
			assert.NotNil(t, err, desc)
		}
	}
}

type fixedCache struct {
	putKey string
	putErr error
	getKey string
	getErr error
	value  []byte
}

func (f *fixedCache) Put(key string, value []byte) error {
	f.putKey = key
	f.value = value
	return f.putErr
}

func (f *fixedCache) Get(key string) ([]byte, error) {
	f.getKey = key
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.value, nil
}

type fixedAcquirer struct {
	docKey id.ID
	doc    *libriapi.Document
	err    error
}

func (f *fixedAcquirer) Acquire(
	docKey id.ID, authorPub []byte, lc libriapi.Getter,
) (*libriapi.Document, error) {
	f.docKey = docKey
	return f.doc, f.err
}
