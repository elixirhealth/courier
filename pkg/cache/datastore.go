package cache

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"time"
	"bytes"
	"github.com/drausin/libri/libri/common/id"
)

const (
	documentKind     = "document"
	accessRecordKind = "access_record"
	keySize          = 2 * id.Length // hex length of document IDs
	maxValuePartSize = 1024 * 1024
)

var (
	// ErrInvalidKeySize indicates when the key is not the expected length.
	ErrInvalidKeySize = errors.New("invalid key size")

	// ErrValueTooLarge indicates when the value is too large to be stored.
	ErrValueTooLarge = errors.New("value too large")

	// ErrExistingNotEqualNewValue indicates when the existing stored value is not the same as
	// the new value,
	// violating the cache's immutability assumption.
	ErrExistingNotEqualNewValue = errors.New("existing value does not equal new value")
)

// MarshaledDocument contains a marshaled Libri api.Document split into up to three parts (to obey
// DataStore's max property size of roughly 1MB).
type MarshaledDocument struct {
	ValuePart1 []byte `datastore:"value_part_1,noindex"`
	ValuePart2 []byte `datastore:"value_part_2,noindex,omitempty"`
	ValuePart3 []byte `datastore:"value_part_3,noindex,omitempty"`
}

// AccessRecord contains access times for Puts and Gets for a particular document.
type AccessRecord struct {
	CachePutTimeEarliest time.Time `datastore:"cache_put_time_earliest,noindex"`
	LibriPutTimeEarliest time.Time `datastore:"libri_put_time_earliest,noindex"`
	CacheGetTimeLatest   time.Time `datastore:"cache_get_time_latest,noindex"`
}

type datastoreCache struct {
	client         datastoreClient
	accessRecorder AccessRecorder
}

// NewDatastore creates a new GCP DataStore Cache instance. This function assumes the following:
// - if DATASTORE_EMULATOR_HOST env var is set, it uses that instead of project
// - production creds use GOOGLE_APPLICATION_CREDENTIALS env var to point to the credentials JSON
// file
func NewDatastore(gcpProjectID string) (Cache, error) {
	client, err := datastore.NewClient(context.Background(), gcpProjectID)
	if err != nil {
		return nil, err
	}
	wrappedClient := &datastoreClientImpl{client}
	return &datastoreCache{
		client: wrappedClient,
		accessRecorder: &datastoreAccessRecorder{wrappedClient},
	}, nil
}

// Put stores the marshaled document value at the hex of its key.
func (c *datastoreCache) Put(key string, value []byte) error {
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
		return nil
	}
	docValue, err := splitValue(value)
	if err != nil {
		return err
	}
	if _, err := c.client.put(dsKey, docValue); err != nil {
		return err
	}
	return c.accessRecorder.CachePut(key)
}

// Get retrieves the marshaled document value of the given hex key.
func (c *datastoreCache) Get(key string) ([]byte, error) {
	cacheKey := datastore.NameKey(documentKind, key, nil)
	existingCacheValue := &MarshaledDocument{}
	if err := c.client.get(cacheKey, existingCacheValue); err != nil {
		return nil, err
	}
	if err := c.accessRecorder.CacheGet(key); err != nil {
		return nil, err
	}
	return joinValue(existingCacheValue), nil
}

type datastoreAccessRecorder struct {
	client datastoreClient
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
	value := &AccessRecord{CachePutTimeEarliest: time.Now()}
	_, err = r.client.put(dsKey, value)
	return err
}

// CacheGet updates the access record's latest get time for the document with the given key.
func (r *datastoreAccessRecorder) CacheGet(key string) error {
	return r.update(key, &AccessRecord{CacheGetTimeLatest: time.Now()})
}

// LibriPut updates the access record's latest libri put time.
func (r *datastoreAccessRecorder) LibriPut(key string) error {
	return r.update(key, &AccessRecord{LibriPutTimeEarliest: time.Now()})
}

func (r *datastoreAccessRecorder) update(key string, update *AccessRecord) error {
	dsKey := datastore.NameKey(accessRecordKind, key, nil)
	value := &AccessRecord{}
	if err := r.client.get(dsKey, value); err != nil {
		return err
	}
	if !update.CacheGetTimeLatest.IsZero() {
		value.CacheGetTimeLatest = update.CacheGetTimeLatest
	}
	if !update.LibriPutTimeEarliest.IsZero() {
		value.LibriPutTimeEarliest = update.LibriPutTimeEarliest
	}
	if _, err := r.client.put(dsKey, value); err != nil {
		return err
	}
	return nil
}

type datastoreClient interface {
	put(key *datastore.Key, value interface{}) (*datastore.Key, error)
	get(key *datastore.Key, dest interface{}) error
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
			ValuePart2: value[maxValuePartSize:2*maxValuePartSize],
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
