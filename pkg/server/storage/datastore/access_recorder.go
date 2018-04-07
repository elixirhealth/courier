package datastore

import (
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
	accessRecordKind = "access_record"
)

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
