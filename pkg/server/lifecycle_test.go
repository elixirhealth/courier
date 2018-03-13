package server

import (
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	errors2 "github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/id"
	"github.com/drausin/libri/libri/common/subscribe"
	"github.com/drausin/libri/libri/librarian/api"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elxirhealth/catalog/pkg/catalogapi"
	"github.com/elxirhealth/service-base/pkg/util"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	okConfig = NewDefaultConfig().
		WithLibrarianAddrs([]*net.TCPAddr{{IP: net.ParseIP("localhost"), Port: 20100}})
)

func init() {
	okConfig.SubscribeTo.NSubscriptions = 0
}

func TestStart(t *testing.T) {
	up := make(chan *Courier, 1)
	wg1 := new(sync.WaitGroup)
	wg1.Add(1)
	go func(wg2 *sync.WaitGroup) {
		defer wg2.Done()
		err := Start(okConfig, up)
		errors2.MaybePanic(err)
	}(wg1)

	c := <-up
	assert.NotNil(t, c)

	c.StopServer()
	wg1.Wait()
}

func TestCourier_startEvictor(t *testing.T) {
	config := NewDefaultConfig().
		WithLibrarianAddrs([]*net.TCPAddr{{IP: net.ParseIP("localhost"), Port: 20100}})
	config.Cache.EvictionPeriod = 10 * time.Millisecond

	c, err := newCourier(config)
	assert.Nil(t, err)
	testCache := &fixedCache{}
	c.cache = testCache

	go c.startEvictor()
	time.Sleep(4 * c.config.Cache.EvictionPeriod)
	close(c.BaseServer.Stop)

	testCache.mu.Lock()
	assert.True(t, testCache.evictCalls > 0)
	testCache.mu.Unlock()
}

func TestCourier_startLibriPutter_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	c, err := newCourier(okConfig)
	assert.Nil(t, err)
	doc, key := api.NewTestDocument(rng)
	docBytes, err := proto.Marshal(doc)
	assert.Nil(t, err)

	c.cache = &fixedCache{
		value: docBytes,
	}
	testAccessRecorder := &fixedAccessRecorder{}
	c.accessRecorder = testAccessRecorder
	testPub := &fixedPublisher{}
	c.libriPublisher = testPub
	c.libriPutQueue <- key.String()
	go c.Serve(func(s *grpc.Server) {}, func() {})
	c.WaitUntilStarted()

	wg1 := new(sync.WaitGroup)
	wg1.Add(1)
	go func(wg2 *sync.WaitGroup) {
		defer wg2.Done()
		c.startLibriPutters()
	}(wg1)
	time.Sleep(25 * time.Millisecond)
	c.StopServer()

	wg1.Wait()
	assert.True(t, testPub.nPubs > 0)
	assert.True(t, testAccessRecorder.nLibriPuts > 0)
}

func TestCourier_startLibriPutter_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	doc, key := api.NewTestDocument(rng)
	docBytes, err := proto.Marshal(doc)
	assert.Nil(t, err)

	// check enough cache get errors should cause it to stop
	c, err := newCourier(okConfig)
	assert.Nil(t, err)
	testPub := &fixedPublisher{}
	c.cache = &fixedCache{
		getErr: errors.New("some cache Get error"),
	}
	c.libriPublisher = testPub
	for i := 0; i < libriPutterErrQueueSize; i++ {
		c.libriPutQueue <- key.String()
	}
	go c.Serve(func(s *grpc.Server) {}, func() {})
	c.WaitUntilStarted()
	c.startLibriPutters()
	assert.Equal(t, uint(0), testPub.nPubs)

	// check fatal unmarshal error
	c, err = newCourier(okConfig)
	assert.Nil(t, err)
	c.cache = &fixedCache{value: []byte{1, 2, 3, 4}}
	c.libriPublisher = &fixedPublisher{}
	for i := 0; i < libriPutterErrQueueSize; i++ {
		c.libriPutQueue <- "some key"
	}
	go c.Serve(func(s *grpc.Server) {}, func() {})
	c.WaitUntilStarted()
	c.startLibriPutters()
	assert.Equal(t, uint(0), testPub.nPubs)

	// check enough publish errors should cause it to stop
	c, err = newCourier(okConfig)
	assert.Nil(t, err)
	testPub = &fixedPublisher{
		err: errors.New("some Publish error"),
	}
	c.cache = &fixedCache{value: docBytes}
	c.libriPublisher = testPub
	for i := 0; i < libriPutterErrQueueSize; i++ {
		c.libriPutQueue <- "some key"
	}
	go c.Serve(func(s *grpc.Server) {}, func() {})
	c.WaitUntilStarted()
	c.startLibriPutters()
	for i := 0; i < libriPutterErrQueueSize; i++ {
		// check that publish errors put keys back onto the queue
		<-c.libriPutQueue
	}
	assert.Equal(t, uint(0), testPub.nPubs)

	// check enough libri put errors should cause it to stop
	c, err = newCourier(okConfig)
	assert.Nil(t, err)
	c.cache = &fixedCache{value: docBytes}
	testPub = &fixedPublisher{}
	c.libriPublisher = testPub
	testAR := &fixedAccessRecorder{
		libriPutErr: errors.New("some access recorder libri put error"),
	}
	c.accessRecorder = testAR
	for i := 0; i < libriPutterErrQueueSize; i++ {
		c.libriPutQueue <- "some key"
	}
	go c.Serve(func(s *grpc.Server) {}, func() {})
	c.WaitUntilStarted()
	c.startLibriPutters()
	assert.True(t, testPub.nPubs > 0)
	assert.Equal(t, uint(0), testAR.nLibriPuts)
}

func TestCourier_startCatalogPutters_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	c, err := newCourier(okConfig)
	assert.Nil(t, err)

	catalogClient := &fixedCatalogClient{}
	c.catalog = catalogClient
	pub := &libriapi.Publication{
		EnvelopeKey:     util.RandBytes(rng, 32),
		EntryKey:        util.RandBytes(rng, 32),
		AuthorPublicKey: util.RandBytes(rng, 33),
		ReaderPublicKey: util.RandBytes(rng, 33),
	}
	c.catalogPutQueue <- &subscribe.KeyedPub{Value: pub}

	go c.Serve(func(s *grpc.Server) {}, func() {})
	c.WaitUntilStarted()

	wg1 := new(sync.WaitGroup)
	wg1.Add(1)
	go func(wg2 *sync.WaitGroup) {
		defer wg2.Done()
		c.startCatalogPutters()
	}(wg1)
	time.Sleep(25 * time.Millisecond)
	c.StopServer()

	wg1.Wait()
	assert.Equal(t, 1, catalogClient.nPuts)
}

func TestCourier_startCatalogPutters_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	pub := &libriapi.Publication{
		EnvelopeKey:     util.RandBytes(rng, 32),
		EntryKey:        util.RandBytes(rng, 32),
		AuthorPublicKey: util.RandBytes(rng, 33),
		ReaderPublicKey: util.RandBytes(rng, 33),
	}

	// check enough catalog put errors should cause it to stop
	c, err := newCourier(okConfig)
	assert.Nil(t, err)
	catalogClient := &fixedCatalogClient{
		putErr: errors.New("some Put error"),
	}
	c.catalog = catalogClient
	for i := 0; i < catalogPutterErrQueueSize; i++ {
		c.catalogPutQueue <- &subscribe.KeyedPub{
			Key:   id.NewPseudoRandom(rng),
			Value: pub,
		}
	}
	go c.Serve(func(s *grpc.Server) {}, func() {})
	c.WaitUntilStarted()
	c.startCatalogPutters()
	for i := 0; i < catalogPutterErrQueueSize; i++ {
		// check that publish errors put keys back onto the queue
		<-c.catalogPutQueue
	}
}

type fixedPublisher struct {
	nPubs uint
	err   error
	mu    sync.Mutex
}

func (f *fixedPublisher) Publish(
	doc *api.Document, authorPub []byte, lc api.Putter,
) (id.ID, error) {
	if f.err != nil {
		return nil, f.err
	}
	docKey, _ := api.GetKey(doc)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nPubs++
	return docKey, nil
}

type fixedAccessRecorder struct {
	cachePutErr         error
	cacheGetErr         error
	cacheEvict          error
	libriPutErr         error
	nextEvictions       []string
	getEvictionBatchErr error
	nLibriPuts          uint
	evictErr            error
}

func (r *fixedAccessRecorder) Evict(keys []string) error {
	return r.evictErr
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
	if r.libriPutErr != nil {
		return r.libriPutErr
	}
	r.nLibriPuts++
	return nil
}

func (r *fixedAccessRecorder) GetNextEvictions() ([]string, error) {
	return r.nextEvictions, r.getEvictionBatchErr
}

type fixedCatalogClient struct {
	putRp  *catalogapi.PutResponse
	putErr error
	nPuts  int
	mu     sync.Mutex
}

func (f *fixedCatalogClient) Put(
	ctx context.Context, in *catalogapi.PutRequest, opts ...grpc.CallOption,
) (*catalogapi.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nPuts++
	return f.putRp, f.putErr
}

func (f *fixedCatalogClient) Search(
	ctx context.Context, in *catalogapi.SearchRequest, opts ...grpc.CallOption,
) (*catalogapi.SearchResponse, error) {
	panic("implement me")
}
