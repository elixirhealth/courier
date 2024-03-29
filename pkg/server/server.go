package server

import (
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
	catalogclient "github.com/elixirhealth/catalog/pkg/client"
	api "github.com/elixirhealth/courier/pkg/courierapi"
	"github.com/elixirhealth/courier/pkg/server/storage"
	keyclient "github.com/elixirhealth/key/pkg/client"
	"github.com/elixirhealth/service-base/pkg/server"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrDocumentNotFound indicates when a document is found in neither the Storage nor libri
	// for a given key.
	ErrDocumentNotFound = status.Error(codes.NotFound, "document not found")

	// ErrFullLibriPutQueue indicates when the libri put queue is full.
	ErrFullLibriPutQueue = errors.New("full libri Put queue")

	// ErrInternal represents an internal error (e.g., with storage or dependency service call).
	ErrInternal = status.Error(codes.Internal, "internal error")

	errMissingCatalog = errors.New("missing catalog address")
	errMissingKey     = errors.New("missing key address")
)

// Courier implements the CourierServer interface.
type Courier struct {
	*server.BaseServer
	config *Config

	clientID       ecid.ID
	cache          storage.Cache
	accessRecorder storage.AccessRecorder

	libriGetter    libriapi.Getter
	libriPutter    libriapi.Putter
	libriAcquirer  publish.Acquirer
	libriPublisher publish.Publisher
	libriPutQueue  chan []byte

	catalogPutter   catalogPutter
	subscribeTo     subscribe.To
	catalogPutQueue chan *subscribe.KeyedPub
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
	if err != nil {
		return nil, err
	}
	subscribeTo := subscribe.NewTo(config.SubscribeTo, baseServer.Logger, clientID,
		setLibClients, signer, recentPubs, catalogPutQueue)
	pubParams := &publish.Parameters{
		PutTimeout: config.LibriPutTimeout,
		GetTimeout: config.LibriGetTimeout,
	}
	acquirer := publish.NewAcquirer(clientID, signer, pubParams)
	publisher := publish.NewPublisher(clientID, signer, pubParams)
	cp := &catalogPutterImpl{
		config:  config,
		logger:  baseServer.Logger,
		catalog: catalog,
		key:     key,
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
		libriPutQueue:  make(chan []byte, config.LibriPutQueueSize),

		catalogPutter:   cp,
		subscribeTo:     subscribeTo,
		catalogPutQueue: catalogPutQueue,
	}, nil
}

// Put puts a value into the Storage and libri network.
func (c *Courier) Put(ctx context.Context, rq *api.PutRequest) (*api.PutResponse, error) {
	c.Logger.Debug("received Put request", zap.String(logKey, id.Hex(rq.Key)))
	if err := api.ValidatePutRequest(rq); err != nil {
		c.Logger.Info("Put request invalid", zap.String(logErr, err.Error()))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	newDocBytes, err := proto.Marshal(rq.Value)
	cerrors.MaybePanic(err)

	if exists, err2 := c.cache.Put(rq.Key, newDocBytes); err2 != nil {
		c.Logger.Error("cache Put error", zap.Error(err))
		return nil, ErrInternal
	} else if exists {
		c.Logger.Info("left existing document", zap.String(logKey, id.Hex(rq.Key)))
		return &api.PutResponse{}, nil
	}
	if err = c.catalogPutter.maybePut(rq.Key, rq.Value); err != nil {
		c.Logger.Error("maybePut error", zap.Error(err))
		return nil, ErrInternal
	}
	if err = c.maybeAddLibriPutQueue(rq.Key); err != nil {
		c.Logger.Error("maybeAddLibriPutQueue error", zap.Error(err))
		return nil, ErrInternal
	}
	c.Logger.Info("put new document", zap.String(logKey, id.Hex(rq.Key)))
	return &api.PutResponse{}, nil
}

// Get retrieves a value from the Storage or libri network, if it exists.
func (c *Courier) Get(ctx context.Context, rq *api.GetRequest) (*api.GetResponse, error) {
	c.Logger.Debug("received Get request", zap.String(logKey, id.Hex(rq.Key)))
	if err := api.ValidateGetRequest(rq); err != nil {
		c.Logger.Info("Put request invalid", zap.String(logErr, err.Error()))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	docBytes, err := c.cache.Get(rq.Key)
	if err != nil && err != storage.ErrMissingValue {
		// unexpected error
		c.Logger.Error("cache Get error", zap.Error(err))
		return nil, ErrInternal
	}
	if err == nil {
		// Storage has doc
		doc := &libriapi.Document{}
		if err = proto.Unmarshal(docBytes, doc); err != nil {
			c.Logger.Error("document proto unmarshal error", zap.Error(err))
			return nil, ErrInternal
		}
		c.Logger.Info("returning value from cache", zap.String(logKey, id.Hex(rq.Key)))
		return &api.GetResponse{Value: doc}, nil
	}

	// Storage doesn't have value, so try to get it from libri
	keyID := id.FromBytes(rq.Key)
	doc, err := c.libriAcquirer.Acquire(keyID, nil, c.libriGetter)
	if err != nil && err != libriapi.ErrMissingDocument {
		c.Logger.Error("libri acquire error", zap.Error(err))
		return nil, ErrInternal
	}
	if err == libriapi.ErrMissingDocument {
		c.Logger.Info("document not found in libri", zap.String(logKey, id.Hex(rq.Key)))
		return nil, ErrDocumentNotFound
	}
	docBytes, err = proto.Marshal(doc)
	cerrors.MaybePanic(err) // should never happen since we just unmarshaled from wire
	if _, err = c.cache.Put(rq.Key, docBytes); err != nil {
		c.Logger.Info("cache put error", zap.Error(err))
		return nil, ErrInternal
	}
	c.Logger.Info("returning value from libri", zap.String(logKey, id.Hex(rq.Key)))
	return &api.GetResponse{
		Value: doc,
	}, nil
}

func (c *Courier) maybeAddLibriPutQueue(key []byte) error {
	select {
	case <-c.BaseServer.Stop:
		return grpc.ErrServerStopped
	case c.libriPutQueue <- key:
		return nil
	default:
		return ErrFullLibriPutQueue
	}
}
