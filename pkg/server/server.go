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
	"github.com/drausin/libri/libri/common/subscribe"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/drausin/libri/libri/librarian/client"
	"github.com/elixirhealth/catalog/pkg/catalogapi"
	catalogclient "github.com/elixirhealth/catalog/pkg/client"
	"github.com/elixirhealth/courier/pkg/cache"
	api "github.com/elixirhealth/courier/pkg/courierapi"
	keyclient "github.com/elixirhealth/key/pkg/client"
	"github.com/elixirhealth/key/pkg/keyapi"
	"github.com/elixirhealth/service-base/pkg/server"
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

	errMissingCatalog = errors.New("missing catalog address")
	errMissingKey     = errors.New("missing key address")
)

// Courier implements the CourierServer interface.
type Courier struct {
	*server.BaseServer
	config *Config

	clientID       ecid.ID
	cache          cache.Cache
	accessRecorder cache.AccessRecorder

	libriGetter    libriapi.Getter
	libriPutter    libriapi.Putter
	libriAcquirer  publish.Acquirer
	libriPublisher publish.Publisher
	libriPutQueue  chan string

	catalog         catalogapi.CatalogClient
	subscribeTo     subscribe.To
	catalogPutQueue chan *subscribe.KeyedPub

	key keyapi.KeyClient
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

	clients, err := client.NewDefaultLRUPool()
	cerrors.MaybePanic(err) // should never happen
	uniformLibClients, err := client.NewUniformBalancer(config.Librarians, clients, rng)
	if err != nil {
		return nil, err
	}
	setLibClients, err := client.NewSetBalancer(config.Librarians, clients, rng)
	if err != nil {
		return nil, err
	}
	getters := client.NewUniformGetterBalancer(uniformLibClients)
	putters := client.NewUniformPutterBalancer(uniformLibClients)
	getter := client.NewRetryGetter(getters, true, config.LibriGetTimeout)
	putter := client.NewRetryPutter(putters, config.LibriPutTimeout)
	if config.Catalog == nil {
		return nil, errMissingCatalog
	}
	catalog, err := catalogclient.NewInsecure(config.Catalog.String())
	if err != nil {
		return nil, err
	}
	if config.Key == nil {
		return nil, errMissingKey
	}
	key, err := keyclient.NewInsecure(config.Key.String())
	if err != nil {
		return nil, err
	}

	signer := client.NewSigner(clientID.Key())
	catalogPutQueue := make(chan *subscribe.KeyedPub, config.CatalogPutQueueSize)
	recentPubs, err := subscribe.NewRecentPublications(config.SubscribeTo.RecentCacheSize)
	subscribeTo := subscribe.NewTo(config.SubscribeTo, baseServer.Logger, clientID,
		setLibClients, signer, recentPubs, catalogPutQueue)

	pubParams := &publish.Parameters{
		PutTimeout: config.LibriPutTimeout,
		GetTimeout: config.LibriGetTimeout,
	}
	acquirer := publish.NewAcquirer(clientID, signer, pubParams)
	publisher := publish.NewPublisher(clientID, signer, pubParams)

	if err != nil {
		return nil, err
	}
	return &Courier{
		BaseServer: baseServer,
		config:     config,

		clientID:       clientID,
		cache:          c,
		accessRecorder: ar,

		libriGetter:    getter,
		libriPutter:    putter,
		libriAcquirer:  acquirer,
		libriPublisher: publisher,
		libriPutQueue:  make(chan string, config.LibriPutQueueSize),

		catalog:         catalog,
		subscribeTo:     subscribeTo,
		catalogPutQueue: catalogPutQueue,

		key: key,
	}, nil
}

// Put puts a value into the Cache and libri network.
func (c *Courier) Put(ctx context.Context, rq *api.PutRequest) (*api.PutResponse, error) {
	c.Logger.Debug("received Put request", zap.String(logKey, id.Hex(rq.Key)))
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
	if err = c.maybePutCatalog(rq.Key, rq.Value); err != nil {
		return nil, err
	}
	if err = c.maybeAddLibriPutQueue(docKey.String()); err != nil {
		return nil, err
	}
	rp := &api.PutResponse{Operation: api.PutOperation_STORED}
	c.Logger.Info("put document", putDocumentFields(rq, rp)...)
	return rp, nil
}

func (c *Courier) maybePutCatalog(key []byte, value *libriapi.Document) error {
	switch ct := value.Contents.(type) {
	case *libriapi.Document_Envelope:
		// create publication receipt for catalog from envelope
		pr := &catalogapi.PublicationReceipt{
			EnvelopeKey:     key,
			EntryKey:        ct.Envelope.EntryKey,
			AuthorPublicKey: ct.Envelope.AuthorPublicKey,
			ReaderPublicKey: ct.Envelope.ReaderPublicKey,
		}
		if err := c.putCatalog(pr); err != nil {
			return err
		}
	default:
		// do nothing for Entry or Page
	}
	return nil
}

func (c *Courier) maybeAddLibriPutQueue(key string) error {
	select {
	case <-c.BaseServer.Stop:
		return grpc.ErrServerStopped
	case c.libriPutQueue <- key:
		return nil
	default:
		return ErrFullLibriPutQueue
	}
}

// Get retrieves a value from the Cache or libri network, if it exists.
func (c *Courier) Get(ctx context.Context, rq *api.GetRequest) (*api.GetResponse, error) {
	c.Logger.Debug("received Get request", zap.String(logKey, id.Hex(rq.Key)))
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
	doc, err := c.libriAcquirer.Acquire(docKey, nil, c.libriGetter)
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
