package datastore

import (
	"bytes"
	"context"
	"encoding/hex"

	"cloud.google.com/go/datastore"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"go.uber.org/zap"
)

const (
	documentKind     = "document"
	maxValuePartSize = 1024 * 1024
)

// MarshaledDocument contains a marshaled Libri api.Document split into up to three parts (to obey
// DataStore's max property size of roughly 1MB).
type MarshaledDocument struct {
	ValuePart1 []byte `datastore:"value_part_1,noindex"`
	ValuePart2 []byte `datastore:"value_part_2,noindex,omitempty"`
	ValuePart3 []byte `datastore:"value_part_3,noindex,omitempty"`
}

type cache struct {
	params         *storage.Parameters
	client         bstorage.DatastoreClient
	accessRecorder storage.AccessRecorder
	logger         *zap.Logger
}

// New creates a new GCP DataStore Cache instance. This function assumes the following:
// - if DATASTORE_EMULATOR_HOST env var is set, it uses that instead of project
// - production creds use GOOGLE_APPLICATION_CREDENTIALS env var to point to the credentials JSON
// file
func New(
	gcpProjectID string, params *storage.Parameters, logger *zap.Logger,
) (storage.Cache, storage.AccessRecorder, error) {
	client, err := datastore.NewClient(context.Background(), gcpProjectID)
	if err != nil {
		return nil, nil, err
	}
	wrappedClient := &bstorage.DatastoreClientImpl{Inner: client}
	ar := &accessRecorder{
		client: wrappedClient,
		iter:   &datastoreIteratorImpl{},
		params: params,
		logger: logger,
	}
	return &cache{
		params:         params,
		client:         wrappedClient,
		accessRecorder: ar,
		logger:         logger,
	}, ar, nil
}

// Put stores the marshaled document value at the hex of its Key.
func (c *cache) Put(key []byte, value []byte) (bool, error) {
	keyHex := hex.EncodeToString(key)
	logger := c.logger.With(zap.String(logKey, keyHex))
	logger.Debug("putting into cache")
	if len(key) != storage.KeySize {
		return false, storage.ErrInvalidKeySize
	}
	if len(value) > storage.MaxValueSize {
		return false, storage.ErrValueTooLarge
	}
	dsKey := datastore.NameKey(documentKind, keyHex, nil)
	existingValue := &MarshaledDocument{}
	ctx, cancel := context.WithTimeout(context.Background(), c.params.GetTimeout)
	err := c.client.Get(ctx, dsKey, existingValue)
	cancel()
	if err != nil && err != datastore.ErrNoSuchEntity {
		return false, err
	}
	if err == nil {
		// value exists
		if !bytes.Equal(value, joinValue(existingValue)) {
			return true, storage.ErrExistingNotEqualNewValue
		}
		logger.Debug("cache already contains value")
		return true, nil
	}
	docValue, err := splitValue(value)
	if err != nil {
		return false, err
	}
	ctx, cancel = context.WithTimeout(context.Background(), c.params.PutTimeout)
	if _, err = c.client.Put(ctx, dsKey, docValue); err != nil {
		cancel()
		return false, err
	}
	cancel()
	if err = c.accessRecorder.CachePut(key); err != nil {
		return false, err
	}
	logger.Debug("put into cache")
	return false, nil
}

// Get retrieves the marshaled document value of the given hex Key.
func (c *cache) Get(key []byte) ([]byte, error) {
	keyHex := hex.EncodeToString(key)
	logger := c.logger.With(zap.String(logKey, keyHex))
	logger.Debug("getting from cache")
	cacheKey := datastore.NameKey(documentKind, keyHex, nil)
	existingCacheValue := &MarshaledDocument{}
	ctx, cancel := context.WithTimeout(context.Background(), c.params.GetTimeout)
	defer cancel()
	if err := c.client.Get(ctx, cacheKey, existingCacheValue); err != nil {
		if err == datastore.ErrNoSuchEntity {
			logger.Debug("cache does not have value")
			return nil, storage.ErrMissingValue
		}
		return nil, err
	}
	if err := c.accessRecorder.CacheGet(key); err != nil {
		return nil, err
	}
	logger.Debug("got value from cache")
	return joinValue(existingCacheValue), nil
}

// EvictNext removes the next batch of documents eligible for eviction from the cache.
func (c *cache) EvictNext() error {
	c.logger.Debug("beginning next eviction")
	keys, err := c.accessRecorder.GetNextEvictions()
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		c.logger.Debug("evicted no documents")
		return nil
	}
	dsKeys := make([]*datastore.Key, len(keys))
	for i, key := range keys {
		dsKeys[i] = datastore.NameKey(documentKind, hex.EncodeToString(key), nil)
	}
	if err = c.accessRecorder.CacheEvict(keys); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.params.DeleteTimeout)
	defer cancel()
	if err = c.client.Delete(ctx, dsKeys); err != nil {
		return err
	}
	c.logger.Info("evicted documents", zap.Int(logNEvicted, len(dsKeys)))
	return nil
}

// Close cleans up an resources held by the cache.
func (c *cache) Close() error {
	return nil
}

func splitValue(value []byte) (*MarshaledDocument, error) {
	if len(value) <= maxValuePartSize {
		return &MarshaledDocument{
			ValuePart1: value,
		}, nil
	}
	if len(value) > maxValuePartSize && len(value) <= 2*maxValuePartSize {
		return &MarshaledDocument{
			ValuePart1: value[:maxValuePartSize],
			ValuePart2: value[maxValuePartSize:],
		}, nil
	}
	if len(value) <= 3*maxValuePartSize {
		return &MarshaledDocument{
			ValuePart1: value[:maxValuePartSize],
			ValuePart2: value[maxValuePartSize : 2*maxValuePartSize],
			ValuePart3: value[2*maxValuePartSize:],
		}, nil
	}
	return nil, storage.ErrValueTooLarge
}

func joinValue(cacheValue *MarshaledDocument) []byte {
	value := make([]byte, 0)
	if cacheValue.ValuePart1 != nil {
		value = append(value, cacheValue.ValuePart1...)
	}
	if cacheValue.ValuePart2 != nil {
		value = append(value, cacheValue.ValuePart2...)
	}
	if cacheValue.ValuePart3 != nil {
		value = append(value, cacheValue.ValuePart3...)
	}
	return value
}
