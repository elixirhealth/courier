package server

import (
	"github.com/elxirhealth/courier/pkg/api"
	"context"
	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/drausin/libri/libri/common/id"
	"github.com/golang/protobuf/proto"
	"github.com/drausin/libri/libri/common/ecid"
	"github.com/drausin/libri/libri/author/io/publish"
	lapi "github.com/drausin/libri/libri/librarian/api"
	"errors"
	"github.com/elxirhealth/courier/pkg/util"
	"bytes"
)

var (
	ErrDocumentNotFound = errors.New("document not found")

	ErrExistingNotEqualNewDocument = errors.New("existing does not equal new document")

	ErrFullPutQueue = errors.New("full Put queue")
)

type courier struct {
	clientID  ecid.ID
	cache     cache.Cache
	getter    lapi.Getter
	putter    lapi.Putter
	acquirer  publish.Acquirer
	publisher publish.Publisher
	toPut     chan string
	config    *Config
	// health server
	// metrics server
	// stop chan
	// stopped chan
	// logger
}

func (c *courier) Get(ctx context.Context, rq *api.GetRequest) (*api.GetResponse, error) {
	// validate request

	docKey := id.FromBytes(rq.Key)
	docBytes, err := c.cache.Get(docKey.String());
	if err != nil && err != cache.ErrMissingValue {
		// unexpected error
		return nil, err
	}
	if err == nil {
		// cache has doc
		doc := &lapi.Document{}
		if err := proto.Unmarshal(docBytes, doc); err != nil {
			return nil, err
		}
		return &api.GetResponse{Value: doc}, nil
	}

	// cache doesn't have value, so try to get it from libri
	doc, err := c.acquirer.Acquire(docKey, nil, c.getter)
	if err != nil && err != api.ErrMissingDocument {
		return nil, err
	}
	if err == api.ErrMissingDocument {
		return nil, ErrDocumentNotFound
	}
	docBytes, err = proto.Marshal(doc)
	util.MaybePanic(err) // should never happen since we just unmarshaled from wire
	if err = c.cache.Put(docKey.String(), docBytes); err != nil {
		return nil, err
	}
	return &api.GetResponse{Value: doc}, nil
}

func (c *courier) Put(ctx context.Context, rq *api.PutRequest) (*api.PutResponse, error) {
	// validate request

	// check cache for value
	docKey := id.FromBytes(rq.Key)
	cachedDocBytes, err := c.cache.Get(docKey.String());
	if err != nil && err != cache.ErrMissingValue {
		// unexpected error
		return nil, err
	}
	newDocBytes, err := proto.Marshal((*lapi.Document)(rq.Value))
	util.MaybePanic(err) // should never happen since we just unmarshaled from wire
	if err == nil {
		// cache has doc
		if !bytes.Equal(newDocBytes, cachedDocBytes) {
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
		return nil, ErrFullPutQueue
	}
}
