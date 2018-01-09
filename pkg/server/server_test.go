package server

import (
	"context"
	"math/rand"
	"testing"

	"github.com/drausin/libri/libri/common/id"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elxirhealth/courier/pkg/cache"
	api "github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

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
		cache: cc,
		toPut: make(chan string, 1),
	}
	rp, err := c.Put(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, api.PutOperation_LEFT_EXISTING, rp.Operation)
	assert.Equal(t, key.String(), cc.getKey)

	// when cache doesn't have value, Put request should store in cache and add
	// to toPut queue
	cc = &fixedCache{getErr: cache.ErrMissingValue}
	c = &courier{
		cache: cc,
		toPut: make(chan string, 1),
	}
	rp, err = c.Put(context.Background(), rq)
	assert.Nil(t, err)
	assert.Equal(t, api.PutOperation_STORED, rp.Operation)
	assert.Equal(t, key.String(), cc.getKey)
	assert.Equal(t, key.String(), cc.putKey)
	assert.Equal(t, valueBytes, cc.value)
	toPutKey := <-c.toPut
	assert.Equal(t, key.String(), toPutKey)
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
