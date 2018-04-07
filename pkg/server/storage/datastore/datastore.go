package datastore

import (
	"bytes"
	"container/heap"
	"context"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

const (
	documentKind     = "document"
	accessRecordKind = "access_record"
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

// New creates a new GCP DataStore Storage instance. This function assumes the following:
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
func (c *cache) Put(key string, value []byte) error {
	logger := c.logger.With(zap.String("Key", key))
	logger.Debug("putting into cache")
	if len(key) != storage.KeySize {
		return storage.ErrInvalidKeySize
	}
	dsKey := datastore.NameKey(documentKind, key, nil)
	existingValue := &MarshaledDocument{}
	ctx, cancel := context.WithTimeout(context.Background(), c.params.GetTimeout)
	err := c.client.Get(ctx, dsKey, existingValue)
	cancel()
	if err != nil && err != datastore.ErrNoSuchEntity {
		return err
	}
	if err == nil {
		// value exists
		if !bytes.Equal(value, joinValue(existingValue)) {
			return storage.ErrExistingNotEqualNewValue
		}
		logger.Debug("cache already contains value")
		return nil
	}
	docValue, err := splitValue(value)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), c.params.PutTimeout)
	if _, err = c.client.Put(ctx, dsKey, docValue); err != nil {
		cancel()
		return err
	}
	cancel()
	if err = c.accessRecorder.CachePut(key); err != nil {
		return err
	}
	logger.Debug("put into cache")
	return nil
}

// Get retrieves the marshaled document value of the given hex Key.
func (c *cache) Get(key string) ([]byte, error) {
	logger := c.logger.With(zap.String("Key", key))
	logger.Debug("getting from cache")
	cacheKey := datastore.NameKey(documentKind, key, nil)
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
	keyNames, err := c.accessRecorder.GetNextEvictions()
	if err != nil {
		return err
	}
	if len(keyNames) == 0 {
		c.logger.Debug("evicted no documents")
		return nil
	}
	dsKeys := make([]*datastore.Key, len(keyNames))
	for i, keyName := range keyNames {
		dsKeys[i] = datastore.NameKey(documentKind, keyName, nil)
	}
	if err = c.accessRecorder.Evict(keyNames); err != nil {
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

type accessRecorder struct {
	client bstorage.DatastoreClient
	iter   datastoreIterator
	params *storage.Parameters
	logger *zap.Logger
}

// CachePut creates a new access record with the cache's put time for the document with the given
// Key.
func (r *accessRecorder) CachePut(key string) error {
	dsKey := datastore.NameKey(accessRecordKind, key, nil)
	ctx, cancel := context.WithTimeout(context.Background(), r.params.GetTimeout)
	err := r.client.Get(ctx, dsKey, &storage.AccessRecord{})
	cancel()
	if err != datastore.ErrNoSuchEntity {
		// either real error or get worked fine (and err is nil), so record already exists;
		// in both cases, we just want to return the err
		return err
	}
	value := storage.NewCachePutAccessRecord()
	ctx, cancel = context.WithTimeout(context.Background(), r.params.PutTimeout)
	defer cancel()
	if _, err = r.client.Put(ctx, dsKey, value); err != nil {
		return err
	}
	r.logger.Debug("put new access record", accessRecordFields(key, value)...)
	return nil
}

// CacheGet updates the access record's latest get time for the document with the given Key.
func (r *accessRecorder) CacheGet(key string) error {
	return r.update(key, &storage.AccessRecord{CacheGetTimeLatest: time.Now()})
}

// LibriPut updates the access record's latest libri put time.
func (r *accessRecorder) LibriPut(key string) error {
	return r.update(key, &storage.AccessRecord{
		LibriPutOccurred:     true,
		LibriPutTimeEarliest: time.Now(),
	})
}

// CacheEvict deletes the access record for the documents with the given keys.
func (r *accessRecorder) CacheEvict(keys []string) error {
	dsKeys := make([]*datastore.Key, len(keys))
	for i, key := range keys {
		dsKeys[i] = datastore.NameKey(accessRecordKind, key, nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.params.DeleteTimeout)
	defer cancel()
	if err := r.client.Delete(ctx, dsKeys); err != nil {
		return err
	}
	r.logger.Debug("evicted access records", zap.Int(logNValues, len(keys)))
	return nil
}

// GetNextEvictions gets the next batch of keys for documents to evict.
func (r *accessRecorder) GetNextEvictions() ([]string, error) {
	beforeDate := time.Now().Unix()/storage.SecsPerDay - int64(r.params.RecentWindowDays)

	r.logger.Debug("finding evictable values", beforeDateFields(beforeDate)...)
	evictable := datastore.NewQuery(accessRecordKind).
		Filter("cache_put_date_earliest < ", beforeDate).
		Filter("libri_put_occurred = ", true)

	ctx, cancel := context.WithTimeout(context.Background(), r.params.EvictionQueryTimeout)
	nEvictable, err := r.client.Count(ctx, evictable)
	cancel()
	if err != nil {
		return nil, err
	}
	if nEvictable <= int(r.params.LRUCacheSize) {
		// don't evict anything since cache size smaller than size limit
		r.logger.Debug("fewer evictable values than cache size",
			nextEvictionsFields(nEvictable, r.params.LRUCacheSize)...)
		return []string{}, nil
	}
	nToEvict := nEvictable - int(r.params.LRUCacheSize)
	if nToEvict > int(r.params.EvictionBatchSize) {
		nToEvict = int(r.params.EvictionBatchSize)
	}
	r.logger.Debug("has evictable values",
		evictableValuesFields(nEvictable, nToEvict, r.params)...)

	// get keys of nToEvict docs gotten least recently
	ctx, cancel = context.WithTimeout(context.Background(), r.params.EvictionQueryTimeout)
	r.iter.Init(r.client.Run(ctx, evictable))
	evict := &storage.KeyGetTimes{}
	var key *datastore.Key
	for {
		record := &storage.AccessRecord{}
		key, err = r.iter.Next(record)
		if err == iterator.Done {
			break
		}
		if err != nil {
			cancel()
			return nil, err
		}
		heap.Push(evict, storage.KeyGetTime{Key: key.Name, GetTime: record.CacheGetTimeLatest})
		if evict.Len() > nToEvict {
			heap.Pop(evict)
		}
	}
	cancel()
	keyNames := make([]string, evict.Len())
	for i, kgt := range *evict {
		keyNames[i] = kgt.Key
	}
	r.logger.Debug("found evictable values",
		nextEvictionsFields(evict.Len(), r.params.LRUCacheSize)...)
	return keyNames, nil
}

func (r *accessRecorder) Evict(keys []string) error {
	dsKeys := make([]*datastore.Key, len(keys))
	for i, keyName := range keys {
		dsKeys[i] = datastore.NameKey(accessRecordKind, keyName, nil)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.params.DeleteTimeout)
	defer cancel()
	if err := r.client.Delete(ctx, dsKeys); err != nil {
		return err
	}
	r.logger.Debug("evicted access records", zap.Int(logNEvicted, len(dsKeys)))
	return nil
}

func (r *accessRecorder) update(key string, update *storage.AccessRecord) error {
	dsKey := datastore.NameKey(accessRecordKind, key, nil)
	existing := &storage.AccessRecord{}
	ctx, cancel := context.WithTimeout(context.Background(), r.params.GetTimeout)
	if err := r.client.Get(ctx, dsKey, existing); err != nil {
		cancel()
		return err
	}
	cancel()
	updated := *existing
	storage.UpdateAccessRecord(&updated, update)
	ctx, cancel = context.WithTimeout(context.Background(), r.params.PutTimeout)
	defer cancel()
	if _, err := r.client.Put(ctx, dsKey, &updated); err != nil {
		return err
	}
	r.logger.Debug("updated access record",
		updatedAccessRecordFields(key, existing, &updated)...)
	return nil
}

type datastoreIterator interface {
	Init(iter *datastore.Iterator)
	Next(dst interface{}) (*datastore.Key, error)
}

type datastoreIteratorImpl struct {
	inner *datastore.Iterator
}

func (i *datastoreIteratorImpl) Init(iter *datastore.Iterator) {
	i.inner = iter
}

func (i *datastoreIteratorImpl) Next(dst interface{}) (*datastore.Key, error) {
	return i.inner.Next(dst)
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
