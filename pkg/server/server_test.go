package server

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/drausin/libri/libri/common/id"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elxirhealth/courier/pkg/base/server"
	"github.com/elxirhealth/courier/pkg/cache"
	api "github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestNewCourier_ok(t *testing.T) {
	config := NewDefaultConfig()
	c, err := newCourier(config)
	assert.Nil(t, err)
	assert.NotNil(t, c.clientID)
	assert.NotNil(t, c.cache)
	assert.NotNil(t, c.getter)
	assert.NotNil(t, c.putter)
	assert.NotNil(t, c.acquirer)
	assert.NotNil(t, c.publisher)
	assert.NotNil(t, c.libriPutQueue)
	assert.Equal(t, config, c.config)
}

func TestNewCourier_err(t *testing.T) {
	badConfigs := map[string]*Config{
		"missing clientID file": NewDefaultConfig().WithClientIDFilepath("missing.der"),
		"emptyProjectID": NewDefaultConfig().WithCache(
			&cache.Parameters{StorageType: cache.DataStore},
		),
		"empty librarian addrs": NewDefaultConfig().WithLibrarianAddrs([]*net.TCPAddr{}),
	}
	for desc, badConfig := range badConfigs {
		c, err := newCourier(badConfig)
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

	// when Cache has value, Put request should leave existing
	cc := &fixedCache{value: valueBytes}
	c := &Courier{
		BaseServer:    server.NewBaseServer(server.NewDefaultBaseConfig()),
		cache:         cc,
		libriPutQueue: make(chan string, 1),
	}
	rp, err := c.Put(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, api.PutOperation_LEFT_EXISTING, rp.Operation)
	assert.Equal(t, key.String(), cc.getKey)

	// when Cache doesn't have value, Put request should store in Cache and add
	// to libriPutQueue queue
	cc = &fixedCache{getErr: cache.ErrMissingValue}
	c = &Courier{
		BaseServer:    server.NewBaseServer(server.NewDefaultBaseConfig()),
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
		c   *Courier
		err error
	}{
		"bad request": {
			rq: &api.PutRequest{},
			c:  &Courier{},
		},
		"Cache Get error": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{getErr: errors.New("some Get error")},
			},
		},
		"existing not equal new doc": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{value: []byte{1, 2, 3, 4}},
			},
			err: ErrExistingNotEqualNewDocument,
		},
		"Cache Put error": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{
					getErr: cache.ErrMissingValue,
					putErr: errors.New("some Put error"),
				},
			},
		},
		"full Libri put queue": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{
					getErr: cache.ErrMissingValue,
				},
				libriPutQueue: make(chan string), // no slack
			},
			err: ErrFullLibriPutQueue,
		},
	}

	for desc, c := range cases {
		c.c.BaseServer = server.NewBaseServer(server.NewDefaultBaseConfig())
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

	// when Cache has doc, Get should return it
	cc := &fixedCache{value: valueBytes}
	c := &Courier{
		BaseServer: server.NewBaseServer(server.NewDefaultBaseConfig()),
		cache:      cc,
	}
	rp, err := c.Get(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, value, rp.Value)
	assert.Equal(t, key.String(), cc.getKey)

	// when Cache doesn't have doc but libri does, Get should return it
	cc = &fixedCache{getErr: cache.ErrMissingValue}
	acq := &fixedAcquirer{doc: value}
	c = &Courier{
		BaseServer: server.NewBaseServer(server.NewDefaultBaseConfig()),
		cache:      cc,
		acquirer:   acq,
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
		c   *Courier
		err error
	}{
		"bad request": {
			rq: &api.GetRequest{},
			c:  &Courier{},
		},
		"Cache Get error": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{getErr: errors.New("some Get error")},
			},
		},
		"bad marshaled doc": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{value: []byte{1, 2, 3, 4}},
			},
		},
		"acquirer Acquire error": {
			rq: okRq,
			c: &Courier{
				cache:    &fixedCache{getErr: cache.ErrMissingValue},
				acquirer: &fixedAcquirer{err: errors.New("some Acquire error")},
			},
		},
		"acquirer Acquire missing doc": {
			rq: okRq,
			c: &Courier{
				cache:    &fixedCache{getErr: cache.ErrMissingValue},
				acquirer: &fixedAcquirer{err: libriapi.ErrMissingDocument},
			},
		},
		"Cache Put error": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{
					getErr: cache.ErrMissingValue,
					putErr: errors.New("some Put error"),
				},
				acquirer: &fixedAcquirer{doc: value},
			},
		},
	}

	for desc, c := range cases {
		c.c.BaseServer = server.NewBaseServer(server.NewDefaultBaseConfig())
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
	putKey       string
	putErr       error
	getKey       string
	getErr       error
	evictErr     error
	evictCalls   uint
	evictNextErr error
	value        []byte
	mu           sync.Mutex
}

func (f *fixedCache) Put(key string, value []byte) error {
	f.putKey = key
	f.value = value
	return f.putErr
}

func (f *fixedCache) Get(key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getKey = key
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.value, nil
}

func (f *fixedCache) Evict(key string) error {
	return f.evictErr
}

func (f *fixedCache) EvictNext() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictCalls++
	return f.evictNextErr
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
