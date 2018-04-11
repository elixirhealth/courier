package server

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/drausin/libri/libri/common/id"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	api "github.com/elixirhealth/courier/pkg/courierapi"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/server"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestNewCourier_ok(t *testing.T) {
	c, err := newCourier(okConfig)
	assert.Nil(t, err)
	assert.NotNil(t, c.clientID)
	assert.NotNil(t, c.cache)
	assert.NotNil(t, c.libriGetter)
	assert.NotNil(t, c.libriPutter)
	assert.NotNil(t, c.libriAcquirer)
	assert.NotNil(t, c.libriPublisher)
	assert.NotNil(t, c.libriPutQueue)
	assert.Equal(t, okConfig, c.config)
}

func TestNewCourier_err(t *testing.T) {
	badConfigs := map[string]*Config{
		"missing clientID file": NewDefaultConfig().WithClientIDFilepath("missing.der"),
		"emptyProjectID": NewDefaultConfig().WithCache(
			&storage.Parameters{Type: bstorage.DataStore},
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
	value := &libriapi.Document{
		Contents: &libriapi.Document_Envelope{
			Envelope: libriapi.NewTestEnvelope(rng),
		},
	}
	key, err := libriapi.GetKey(value)
	assert.Nil(t, err)

	valueBytes, err := proto.Marshal(value)
	assert.Nil(t, err)
	rq := &api.PutRequest{
		Key:   key.Bytes(),
		Value: value,
	}

	// when Storage has value, Put request should leave existing
	cc := &fixedCache{putExists: true}
	c := &Courier{
		BaseServer:    server.NewBaseServer(server.NewDefaultBaseConfig()),
		config:        NewDefaultConfig(),
		cache:         cc,
		catalogPutter: &fixedCatalogPutter{},
		libriPutQueue: make(chan []byte, 1),
	}
	_, err = c.Put(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, key.Bytes(), cc.putKey)

	// when Storage doesn't have value, Put request should store in Storage and add
	// to libriPutQueue queue
	cc = &fixedCache{putExists: false}
	cp := &fixedCatalogPutter{}
	c = &Courier{
		BaseServer:    server.NewBaseServer(server.NewDefaultBaseConfig()),
		config:        NewDefaultConfig(),
		cache:         cc,
		catalogPutter: cp,
		libriPutQueue: make(chan []byte, 1),
	}
	_, err = c.Put(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, key.Bytes(), cc.putKey)
	assert.Equal(t, valueBytes, cc.value)
	assert.Equal(t, 1, cp.nMaybePuts)
	toPutKey := <-c.libriPutQueue
	assert.Equal(t, key.Bytes(), toPutKey)
}

func TestCourier_Put_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	doc := &libriapi.Document{
		Contents: &libriapi.Document_Envelope{
			Envelope: libriapi.NewTestEnvelope(rng),
		},
	}
	key, err := libriapi.GetKey(doc)
	assert.Nil(t, err)
	okRq := &api.PutRequest{
		Key:   key.Bytes(),
		Value: doc,
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
		"cache Put error": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{
					putErr: errTest,
				},
			},
			err: ErrInternal,
		},
		"catalog put err": {
			rq: okRq,
			c: &Courier{
				config:        NewDefaultConfig(),
				cache:         &fixedCache{},
				catalogPutter: &fixedCatalogPutter{maybePutErr: errTest},
			},
			err: ErrInternal,
		},
		"full Libri put queue": {
			rq: okRq,
			c: &Courier{
				config:        NewDefaultConfig(),
				cache:         &fixedCache{},
				catalogPutter: &fixedCatalogPutter{},
				libriPutQueue: make(chan []byte), // no slack
			},
			err: ErrInternal,
		},
	}

	for desc, c := range cases {
		c.c.BaseServer = server.NewBaseServer(server.NewDefaultBaseConfig())
		rp, err2 := c.c.Put(context.Background(), c.rq)
		assert.Nil(t, rp, desc)
		if c.err != nil {
			// specific error
			assert.Equal(t, c.err, err2, desc)
		} else {
			// non-nil error
			assert.NotNil(t, err2, desc)
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

	// when Storage has doc, Get should return it
	cc := &fixedCache{value: valueBytes}
	c := &Courier{
		BaseServer: server.NewBaseServer(server.NewDefaultBaseConfig()),
		cache:      cc,
	}
	rp, err := c.Get(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, value, rp.Value)
	assert.Equal(t, key.Bytes(), cc.getKey)

	// when Storage doesn't have doc but libri does, Get should return it
	cc = &fixedCache{getErr: storage.ErrMissingValue}
	acq := &fixedAcquirer{doc: value}
	c = &Courier{
		BaseServer:    server.NewBaseServer(server.NewDefaultBaseConfig()),
		cache:         cc,
		libriAcquirer: acq,
	}
	rp, err = c.Get(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, value, rp.Value)
	assert.Equal(t, key.Bytes(), cc.getKey)
	assert.Equal(t, key.Bytes(), cc.putKey)
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
		"Storage Get error": {
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
		"libriAcquirer Acquire error": {
			rq: okRq,
			c: &Courier{
				cache:         &fixedCache{getErr: storage.ErrMissingValue},
				libriAcquirer: &fixedAcquirer{err: errors.New("some Acquire error")},
			},
		},
		"libriAcquirer Acquire missing doc": {
			rq: okRq,
			c: &Courier{
				cache:         &fixedCache{getErr: storage.ErrMissingValue},
				libriAcquirer: &fixedAcquirer{err: libriapi.ErrMissingDocument},
			},
		},
		"Storage Put error": {
			rq: okRq,
			c: &Courier{
				cache: &fixedCache{
					getErr: storage.ErrMissingValue,
					putErr: errors.New("some Put error"),
				},
				libriAcquirer: &fixedAcquirer{doc: value},
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
	putKey       []byte
	putErr       error
	putExists    bool
	getKey       []byte
	getErr       error
	evictErr     error
	evictCalls   uint
	evictNextErr error
	value        []byte
	mu           sync.Mutex
}

func (f *fixedCache) Put(key []byte, value []byte) (bool, error) {
	f.putKey = key
	f.value = value
	return f.putExists, f.putErr
}

func (f *fixedCache) Get(key []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getKey = key
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.value, nil
}

func (f *fixedCache) Evict(key []byte) error {
	return f.evictErr
}

func (f *fixedCache) EvictNext() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evictCalls++
	return f.evictNextErr
}

func (f *fixedCache) Close() error {
	return nil
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
