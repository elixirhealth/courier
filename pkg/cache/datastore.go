package cache

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/drausin/libri/libri/common/id"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

const (
	documentKind     = "document"
	accessRecordKind = "access_record"
	keySize          = 2 * id.Length // hex length of document IDs
	maxValuePartSize = 1024 * 1024
)

var (
	// ErrMissingValue indicates that a value is missing for a given key.
	ErrMissingValue = errors.New("missing value")

	// ErrInvalidKeySize indicates when the key is not the expected length.
	ErrInvalidKeySize = errors.New("invalid key size")

	// ErrValueTooLarge indicates when the value is too large to be stored.
	ErrValueTooLarge = errors.New("value too large")

	// ErrExistingNotEqualNewValue indicates when the existing stored value is not the same as
	// the new value,
	// violating the cache's immutability assumption.
	ErrExistingNotEqualNewValue = errors.New("existing value does not equal new value")
)

const (
	secsPerDay = 60 * 60 * 24
)

// MarshaledDocument contains a marshaled Libri api.Document split into up to three parts (to obey
// DataStore's max property size of roughly 1MB).
type MarshaledDocument struct {
	ValuePart1 []byte `datastore:"value_part_1,noindex"`
	ValuePart2 []byte `datastore:"value_part_2,noindex,omitempty"`
	ValuePart3 []byte `datastore:"value_part_3,noindex,omitempty"`
}

type datastoreCache struct {
	client         datastoreClient
	accessRecorder AccessRecorder
	logger         *zap.Logger
}

// NewDatastore creates a new GCP DataStore Cache instance. This function assumes the following:
// - if DATASTORE_EMULATOR_HOST env var is set, it uses that instead of project
// - production creds use GOOGLE_APPLICATION_CREDENTIALS env var to point to the credentials JSON
// file
func NewDatastore(
	gcpProjectID string, params *Parameters, logger *zap.Logger,
) (Cache, AccessRecorder, error) {
	client, err := datastore.NewClient(context.Background(), gcpProjectID)
	if err != nil {
		return nil, nil, err
	}
	wrappedClient := &datastoreClientImpl{client}
	ar := &datastoreAccessRecorder{
		client: wrappedClient,
		iter:   &datastoreIteratorImpl{},
		params: params,
		logger: logger,
	}
	return &datastoreCache{
		client:         wrappedClient,
		accessRecorder: ar,
		logger:         logger,
	}, ar, nil
}

// Put stores the marshaled document value at the hex of its key.
func (c *datastoreCache) Put(key string, value []byte) error {
	logger := c.logger.With(zap.String("key", key))
	logger.Debug("putting into cache")
	if len(key) != keySize {
		return ErrInvalidKeySize
	}
	dsKey := datastore.NameKey(documentKind, key, nil)
	existingValue := &MarshaledDocument{}
	err := c.client.get(dsKey, existingValue)
	if err != nil && err != datastore.ErrNoSuchEntity {
		return err
	}
	if err == nil {
		// value exists
		if !bytes.Equal(value, joinValue(existingValue)) {
			return ErrExistingNotEqualNewValue
		}
		logger.Debug("cache already contains value")
		return nil
	}
	docValue, err := splitValue(value)
	if err != nil {
		return err
	}
	if _, err = c.client.put(dsKey, docValue); err != nil {
		return err
	}
	defer logger.Debug("put into cache")
	return c.accessRecorder.CachePut(key)
}

// Get retrieves the marshaled document value of the given hex key.
func (c *datastoreCache) Get(key string) ([]byte, error) {
	logger := c.logger.With(zap.String("key", key))
	logger.Debug("getting from cache")
	cacheKey := datastore.NameKey(documentKind, key, nil)
	existingCacheValue := &MarshaledDocument{}
	if err := c.client.get(cacheKey, existingCacheValue); err != nil {
		if err == datastore.ErrNoSuchEntity {
			logger.Debug("cache does not have value")
			return nil, ErrMissingValue
		}
		return nil, err
	}
	if err := c.accessRecorder.CacheGet(key); err != nil {
		return nil, err
	}
	defer logger.Debug("got value from cache")
	return joinValue(existingCacheValue), nil
}

// EvictNext removes the next batch of documents eligible for eviction from the cache.
func (c *datastoreCache) EvictNext() error {
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
	if err = c.client.delete(dsKeys); err != nil {
		return err
	}
	c.logger.Info("evicted documents", zap.Int(logNEvicted, len(dsKeys)))
	return nil
}

type datastoreAccessRecorder struct {
	client datastoreClient
	iter   datastoreIterator
	params *Parameters
	logger *zap.Logger
}

// CachePut creates a new access record with the cache's put time for the document with the given
// key.
func (r *datastoreAccessRecorder) CachePut(key string) error {
	dsKey := datastore.NameKey(accessRecordKind, key, nil)
	err := r.client.get(dsKey, &AccessRecord{})
	if err != datastore.ErrNoSuchEntity {
		// either real error or get worked fine (and err is nil), so record already exists;
		// in both cases, we just want to return the err
		return err
	}
	value := newCachePutAccessRecord()
	_, err = r.client.put(dsKey, value)
	r.logger.Debug("put new access record",
		zap.String(logKey, key),
		zap.Object(logValue, value),
	)
	return err
}

func newCachePutAccessRecord() *AccessRecord {
	now := time.Now()
	return &AccessRecord{
		CachePutDateEarliest: now.Unix() / secsPerDay,
		CachePutTimeEarliest: now,
		LibriPutOccurred:     false,
	}
}

// CacheGet updates the access record's latest get time for the document with the given key.
func (r *datastoreAccessRecorder) CacheGet(key string) error {
	return r.update(key, &AccessRecord{CacheGetTimeLatest: time.Now()})
}

// LibriPut updates the access record's latest libri put time.
func (r *datastoreAccessRecorder) LibriPut(key string) error {
	return r.update(key, &AccessRecord{
		LibriPutOccurred:     true,
		LibriPutTimeEarliest: time.Now(),
	})
}

// CacheEvict deletes the access record for the documents with the given keys.
func (r *datastoreAccessRecorder) CacheEvict(keys []string) error {
	dsKeys := make([]*datastore.Key, len(keys))
	for i, key := range keys {
		dsKeys[i] = datastore.NameKey(accessRecordKind, key, nil)
	}
	if err := r.client.delete(dsKeys); err != nil {
		return err
	}
	r.logger.Debug("evicted access records", zap.Int(logNValues, len(keys)))
	return nil
}

// GetNextEvictions gets the next batch of keys for documents to evict.
func (r *datastoreAccessRecorder) GetNextEvictions() ([]string, error) {
	beforeDate := time.Now().Add(-r.params.RecentWindow).Unix() / secsPerDay

	r.logger.Debug("finding evictable values", beforeDateFields(beforeDate)...)
	evictable := datastore.NewQuery(accessRecordKind).
		Filter("cache_put_date_earliest < ", beforeDate).
		Filter("libri_put_occurred = ", true)

	ctx, cancel := context.WithTimeout(context.Background(), r.params.EvictionQueryTimeout)
	nEvictable, err := r.client.count(ctx, evictable)
	cancel()
	if err != nil {
		return nil, err
	}
	if nEvictable <= int(r.params.LRUCacheSize) {
		// don't evict anything since cache size smaller than size limit
		r.logger.Debug("fewer evictable values than cache size",
			nextEvictionsFields(nEvictable, r.params.LRUCacheSize)...)
		return []string{}, err
	}
	nToEvict := nEvictable - int(r.params.LRUCacheSize)
	if nToEvict > int(r.params.EvictionBatchSize) {
		nToEvict = int(r.params.EvictionBatchSize)
	}
	r.logger.Debug("has evictable values",
		evictableValuesFields(nEvictable, nToEvict, r.params)...)

	// get keys of nToEvict docs gotten least recently
	ctx, cancel = context.WithTimeout(context.Background(), r.params.EvictionQueryTimeout)
	r.iter.Init(r.client.run(ctx, evictable))
	evict := &keyGetTimes{}
	var key *datastore.Key
	for {
		record := &AccessRecord{}
		key, err = r.iter.Next(record)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		heap.Push(evict, keyGetTime{key: key.Name, getTime: record.CacheGetTimeLatest})
		if evict.Len() > nToEvict {
			heap.Pop(evict)
		}
	}
	cancel()
	keyNames := make([]string, evict.Len())
	for i, kgt := range *evict {
		keyNames[i] = kgt.key
	}
	r.logger.Debug("found evictable values",
		nextEvictionsFields(evict.Len(), r.params.LRUCacheSize)...)
	return keyNames, nil
}

func (r *datastoreAccessRecorder) Evict(keys []string) error {
	dsKeys := make([]*datastore.Key, len(keys))
	for i, keyName := range keys {
		dsKeys[i] = datastore.NameKey(accessRecordKind, keyName, nil)
	}
	if err := r.client.delete(dsKeys); err != nil {
		return err
	}
	r.logger.Debug("evicted access records", zap.Int(logNEvicted, len(dsKeys)))
	return nil
}

func (r *datastoreAccessRecorder) update(key string, update *AccessRecord) error {
	dsKey := datastore.NameKey(accessRecordKind, key, nil)
	existing := &AccessRecord{}
	if err := r.client.get(dsKey, existing); err != nil {
		return err
	}
	updated := *existing
	updateAccessRecord(&updated, update)
	if _, err := r.client.put(dsKey, &updated); err != nil {
		return err
	}
	r.logger.Debug("updated access record",
		updatedAccessRecordFields(key, existing, &updated)...)
	return nil
}

func updateAccessRecord(existing, update *AccessRecord) {
	if !update.CacheGetTimeLatest.IsZero() {
		existing.CacheGetTimeLatest = update.CacheGetTimeLatest
	}
	if !update.LibriPutTimeEarliest.IsZero() {
		existing.LibriPutTimeEarliest = update.LibriPutTimeEarliest
	}
	if update.LibriPutOccurred {
		existing.LibriPutOccurred = update.LibriPutOccurred
	}
}

type datastoreClient interface {
	put(key *datastore.Key, value interface{}) (*datastore.Key, error)
	get(key *datastore.Key, dest interface{}) error
	delete(keys []*datastore.Key) error
	count(ctx context.Context, q *datastore.Query) (int, error)
	run(ctx context.Context, q *datastore.Query) *datastore.Iterator
}

type datastoreClientImpl struct {
	inner *datastore.Client
}

func (c *datastoreClientImpl) get(key *datastore.Key, dest interface{}) error {
	return c.inner.Get(context.Background(), key, dest)
}

func (c *datastoreClientImpl) put(key *datastore.Key, value interface{}) (*datastore.Key, error) {
	return c.inner.Put(context.Background(), key, value)
}

func (c *datastoreClientImpl) delete(keys []*datastore.Key) error {
	return c.inner.DeleteMulti(context.Background(), keys)
}

func (c *datastoreClientImpl) count(ctx context.Context, q *datastore.Query) (int, error) {
	return c.inner.Count(ctx, q)
}

func (c *datastoreClientImpl) run(ctx context.Context, q *datastore.Query) *datastore.Iterator {
	return c.inner.Run(ctx, q)
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
	return nil, ErrValueTooLarge
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
