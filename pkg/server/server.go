package server

import (
	"bytes"
	"context"
	"errors"
	"math/rand"

	"github.com/drausin/libri/libri/author/io/publish"
	"github.com/drausin/libri/libri/common/ecid"
	cerrors "github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/id"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/drausin/libri/libri/librarian/client"
	"github.com/elxirhealth/courier/pkg/cache"
	api "github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/elxirhealth/service-base/pkg/server"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	// ErrDocumentNotFound indicates when a document is found in neither the Cache nor libri
	// for a given key.
	ErrDocumentNotFound = errors.New("document not found")

	// ErrExistingNotEqualNewDocument indicates when existing cached document is not the same
	// as the new document in a Put request.
	ErrExistingNotEqualNewDocument = errors.New("existing does not equal new document")

	// ErrFullLibriPutQueue indicates when the libri put queue is full.
	ErrFullLibriPutQueue = errors.New("full libri Put queue")
)

// Courier implements the CourierServer interface.
type Courier struct {
	*server.BaseServer

	clientID       ecid.ID
	cache          cache.Cache
	accessRecorder cache.AccessRecorder
	getter         libriapi.Getter
	putter         libriapi.Putter
	acquirer       publish.Acquirer
	publisher      publish.Publisher
	libriPutQueue  chan string
	config         *Config
}

// newCourier creates a new CourierServer from the given config.
func newCourier(config *Config) (*Courier, error) {
	baseServer := server.NewBaseServer(config.BaseConfig)
	clientID, err := getClientID(config)
	if err != nil {
		return nil, err
	}
	c, ar, err := getCache(config, baseServer.Logger)
	if err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(clientID.ID().Int().Int64()))
	librarians, err := client.NewUniformBalancer(config.LibrarianAddrs, rng)
	if err != nil {
		return nil, err
	}
	getters := client.NewUniformGetterBalancer(librarians)
	putters := client.NewUniformPutterBalancer(librarians)
	getter := client.NewRetryGetter(getters, true, config.LibriGetTimeout)
	putter := client.NewRetryPutter(putters, config.LibriPutTimeout)

	pubParams := &publish.Parameters{
		PutTimeout: config.LibriPutTimeout,
		GetTimeout: config.LibriGetTimeout,
	}
	acquirer := publish.NewAcquirer(clientID, client.NewSigner(clientID.Key()), pubParams)
	publisher := publish.NewPublisher(clientID, client.NewSigner(clientID.Key()), pubParams)
	return &Courier{
		BaseServer:     baseServer,
		clientID:       clientID,
		cache:          c,
		accessRecorder: ar,
		getter:         getter,
		putter:         putter,
		acquirer:       acquirer,
		publisher:      publisher,
		libriPutQueue:  make(chan string, config.LibriPutQueueSize),
		config:         config,
	}, nil
}

// Put puts a value into the Cache and libri network.
func (c *Courier) Put(ctx context.Context, rq *api.PutRequest) (*api.PutResponse, error) {
	c.Logger.Debug("received put request", zap.String(logKey, id.Hex(rq.Key)))
	if err := api.ValidatePutRequest(rq); err != nil {
		return nil, err
	}
	docKey := id.FromBytes(rq.Key)

	// check Cache for value
	cachedDocBytes, err := c.cache.Get(docKey.String())
	if err != nil && err != cache.ErrMissingValue {
		// unexpected error
		return nil, err
	}
	newDocBytes, err := proto.Marshal(rq.Value)
	cerrors.MaybePanic(err) // should never happen since we just unmarshaled from wire
	if cachedDocBytes != nil {
		// Cache has doc
		if !bytes.Equal(newDocBytes, cachedDocBytes) {
			// *should* never happen, but check just in case
			return nil, ErrExistingNotEqualNewDocument
		}
		rp := &api.PutResponse{Operation: api.PutOperation_LEFT_EXISTING}
		c.Logger.Info("put document", putDocumentFields(rq, rp)...)
		return rp, nil
	}

	// Cache doesn't have doc, so add it
	if err = c.cache.Put(docKey.String(), newDocBytes); err != nil {
		return nil, err
	}
	select {
	case <-c.BaseServer.Stop:
		return nil, grpc.ErrServerStopped
	case c.libriPutQueue <- docKey.String():
		rp := &api.PutResponse{Operation: api.PutOperation_STORED}
		c.Logger.Info("put document", putDocumentFields(rq, rp)...)
		return rp, nil
	default:
		return nil, ErrFullLibriPutQueue
	}
}

// Get retrieves a value from the Cache or libri network, if it exists.
func (c *Courier) Get(ctx context.Context, rq *api.GetRequest) (*api.GetResponse, error) {
	c.Logger.Debug("received get request", zap.String(logKey, id.Hex(rq.Key)))
	if err := api.ValidateGetRequest(rq); err != nil {
		return nil, err
	}

	docKey := id.FromBytes(rq.Key)
	docBytes, err := c.cache.Get(docKey.String())
	if err != nil && err != cache.ErrMissingValue {
		// unexpected error
		return nil, err
	}
	if err == nil {
		// Cache has doc
		doc := &libriapi.Document{}
		if err = proto.Unmarshal(docBytes, doc); err != nil {
			return nil, err
		}
		c.Logger.Info("returning value from cache", zap.String(logKey, id.Hex(rq.Key)))
		return &api.GetResponse{Value: doc}, nil
	}

	// Cache doesn't have value, so try to get it from libri
	doc, err := c.acquirer.Acquire(docKey, nil, c.getter)
	if err != nil && err != libriapi.ErrMissingDocument {
		return nil, err
	}
	if err == libriapi.ErrMissingDocument {
		return nil, ErrDocumentNotFound
	}
	docBytes, err = proto.Marshal(doc)
	cerrors.MaybePanic(err) // should never happen since we just unmarshaled from wire
	if err = c.cache.Put(docKey.String(), docBytes); err != nil {
		return nil, err
	}
	c.Logger.Info("returning value from libri", zap.String(logKey, id.Hex(rq.Key)))
	return &api.GetResponse{
		Value: doc,
	}, nil
}
