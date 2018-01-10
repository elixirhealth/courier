package server

import (
	"bytes"
	"context"
	"errors"

	"github.com/drausin/libri/libri/author/io/publish"
	"github.com/drausin/libri/libri/common/ecid"
	"github.com/drausin/libri/libri/common/id"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elxirhealth/courier/pkg/cache"
	api "github.com/elxirhealth/courier/pkg/courierapi"
	"github.com/elxirhealth/courier/pkg/util"
	"github.com/golang/protobuf/proto"
)

var (
	// ErrDocumentNotFound indicates when a document is found in neither the cache nor libri
	// for a given key.
	ErrDocumentNotFound = errors.New("document not found")

	// ErrExistingNotEqualNewDocument indicates when existing cached document is not the same
	// as the new document in a Put request.
	ErrExistingNotEqualNewDocument = errors.New("existing does not equal new document")

	// ErrFullLibriPutQueue indicates when the libri put queue is full.
	ErrFullLibriPutQueue = errors.New("full libri Put queue")
)

type courier struct {
	clientID  ecid.ID
	cache     cache.Cache
	getter    libriapi.Getter
	putter    libriapi.Putter
	acquirer  publish.Acquirer
	publisher publish.Publisher
	toPut     chan string
	config    *Config
	// stop chan
	// stopped chan/boolean flag
	// health server
	// metrics server
	// logger
}

func (c *courier) Put(ctx context.Context, rq *api.PutRequest) (*api.PutResponse, error) {
	if err := api.ValidatePutRequest(rq); err != nil {
		return nil, err
	}
	docKey := id.FromBytes(rq.Key)

	// check cache for value
	cachedDocBytes, err := c.cache.Get(docKey.String())
	if err != nil && err != cache.ErrMissingValue {
		// unexpected error
		return nil, err
	}
	newDocBytes, err := proto.Marshal(rq.Value)
	util.MaybePanic(err) // should never happen since we just unmarshaled from wire
	if cachedDocBytes != nil {
		// cache has doc
		if !bytes.Equal(newDocBytes, cachedDocBytes) {
			// *should* never happen, but check just in case
			return nil, ErrExistingNotEqualNewDocument
		}
		return &api.PutResponse{Operation: api.PutOperation_LEFT_EXISTING}, nil
	}

	// cache doesn't have doc, so add it
	if err = c.cache.Put(docKey.String(), newDocBytes); err != nil {
		return nil, err
	}
	select {
	case c.toPut <- docKey.String():
		return &api.PutResponse{Operation: api.PutOperation_STORED}, nil
	default:
		return nil, ErrFullLibriPutQueue
	}
}

func (c *courier) Get(ctx context.Context, rq *api.GetRequest) (*api.GetResponse, error) {
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
		// cache has doc
		doc := &libriapi.Document{}
		if err := proto.Unmarshal(docBytes, doc); err != nil {
			return nil, err
		}
		return &api.GetResponse{Value: doc}, nil
	}

	// cache doesn't have value, so try to get it from libri
	doc, err := c.acquirer.Acquire(docKey, nil, c.getter)
	if err != nil && err != libriapi.ErrMissingDocument {
		return nil, err
	}
	if err == libriapi.ErrMissingDocument {
		return nil, ErrDocumentNotFound
	}
	docBytes, err = proto.Marshal(doc)
	util.MaybePanic(err) // should never happen since we just unmarshaled from wire
	if err = c.cache.Put(docKey.String(), docBytes); err != nil {
		return nil, err
	}
	return &api.GetResponse{
		Value: doc,
	}, nil
}
