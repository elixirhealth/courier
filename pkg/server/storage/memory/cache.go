package memory

import (
	"bytes"
	"encoding/hex"
	"sync"

	"github.com/elixirhealth/courier/pkg/server/storage"
	"go.uber.org/zap"
)

type cache struct {
	ar     storage.AccessRecorder
	docs   map[string][]byte
	mu     sync.Mutex
	logger *zap.Logger
}

// New creates a new in-memory cache with the given parameters.
func New(params *storage.Parameters, logger *zap.Logger) (storage.Cache, storage.AccessRecorder) {
	ar := &accessRecorder{
		records: make(map[string]*storage.AccessRecord),
		params:  params,
		logger:  logger,
	}
	return &cache{
		ar:     ar,
		docs:   make(map[string][]byte),
		logger: logger,
	}, ar
}

// Put stores the marshaled document value at the hex of its key.
func (c *cache) Put(key []byte, value []byte) error {
	keyHex := hex.EncodeToString(key)
	logger := c.logger.With(zap.String(logKey, keyHex))
	logger.Debug("putting into cache")
	if len(key) != storage.KeySize {
		return storage.ErrInvalidKeySize
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, in := c.docs[keyHex]; in {
		if !bytes.Equal(value, existing) {
			return storage.ErrExistingNotEqualNewValue
		}
		logger.Debug("cache already contains value")
		return nil
	}
	c.docs[keyHex] = value
	if err := c.ar.CachePut(key); err != nil {
		return err
	}
	logger.Debug("put into cache")
	return nil
}

// Get retrieves the marshaled document value of the given hex key.
func (c *cache) Get(key []byte) ([]byte, error) {
	keyHex := hex.EncodeToString(key)
	logger := c.logger.With(zap.String(logKey, keyHex))
	logger.Debug("getting from cache")
	c.mu.Lock()
	value, in := c.docs[keyHex]
	c.mu.Unlock()
	if !in {
		return nil, storage.ErrMissingValue
	}
	if err := c.ar.CacheGet(key); err != nil {
		return nil, err
	}
	logger.Debug("got value from cache")
	return value, nil
}

// EvictNext removes the next batch of documents eligible for eviction from the cache.
func (c *cache) EvictNext() error {
	c.logger.Debug("beginning next eviction")
	keys, err := c.ar.GetNextEvictions()
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		c.logger.Debug("evicted no documents")
		return nil
	}
	if err = c.ar.CacheEvict(keys); err != nil {
		return err
	}
	for _, key := range keys {
		keyHex := hex.EncodeToString(key)
		c.mu.Lock()
		if _, in := c.docs[keyHex]; !in {
			c.mu.Unlock()
			return storage.ErrMissingValue
		}
		delete(c.docs, keyHex)
		c.mu.Unlock()
	}
	c.logger.Info("evicted documents", zap.Int(logNEvicted, len(keys)))
	return nil
}
