package cache

import (
	"bytes"
	"container/heap"
	"sync"
	"time"
)

type memoryCache struct {
	accessRecorder AccessRecorder
	docs           map[string][]byte
	mu             sync.Mutex
}

// NewMemory creates a new in-memory cache with the given parameters.
func NewMemory(params *Parameters) (Cache, AccessRecorder) {
	ar := &memoryAccessRecorder{
		records: make(map[string]*AccessRecord),
		params:  params,
	}
	return &memoryCache{
		accessRecorder: ar,
		docs:           make(map[string][]byte),
	}, ar
}

func (c *memoryCache) Put(key string, value []byte) error {
	if len(key) != keySize {
		return ErrInvalidKeySize
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, in := c.docs[key]; in {
		if !bytes.Equal(value, existing) {
			return ErrExistingNotEqualNewValue
		}
		return nil
	}
	c.docs[key] = value
	return c.accessRecorder.CachePut(key)
}

func (c *memoryCache) Get(key string) ([]byte, error) {
	value, in := c.docs[key]
	if !in {
		return nil, ErrMissingValue
	}
	if err := c.accessRecorder.CacheGet(key); err != nil {
		return nil, err
	}
	return value, nil
}

func (c *memoryCache) EvictNext() error {
	keys, err := c.accessRecorder.GetNextEvictions()
	if err != nil {
		return err
	}
	if err = c.accessRecorder.Evict(keys); err != nil {
		return err
	}
	for _, key := range keys {
		c.mu.Lock()
		if _, in := c.docs[key]; !in {
			c.mu.Unlock()
			return ErrMissingValue
		}
		delete(c.docs, key)
		c.mu.Unlock()
	}
	return nil
}

type memoryAccessRecorder struct {
	records map[string]*AccessRecord
	params  *Parameters
	mu      sync.Mutex
}

func (r *memoryAccessRecorder) CachePut(key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[key] = newCachePutAccessRecord()
	return nil
}

func (r *memoryAccessRecorder) CacheGet(key string) error {
	return r.update(key, &AccessRecord{CacheGetTimeLatest: time.Now()})
}

func (r *memoryAccessRecorder) CacheEvict(keys []string) error {
	for _, key := range keys {
		r.mu.Lock()
		if _, in := r.records[key]; !in {
			r.mu.Unlock()
			return ErrMissingValue
		}
		delete(r.records, key)
		r.mu.Unlock()
	}
	return nil
}

func (r *memoryAccessRecorder) LibriPut(key string) error {
	return r.update(key, &AccessRecord{
		LibriPutOccurred:     true,
		LibriPutTimeEarliest: time.Now(),
	})
}

func (r *memoryAccessRecorder) GetNextEvictions() ([]string, error) {
	// not trying to be efficient, so just do full scan for docs satisfying eviction criteria
	evict := &keyGetTimes{}
	beforeDate := time.Now().Add(-r.params.RecentWindow).Unix() / secsPerDay
	r.mu.Lock()
	for key, record := range r.records {
		if record.CachePutDateEarliest < beforeDate && record.LibriPutOccurred {
			heap.Push(evict, keyGetTime{key: key, getTime: record.CacheGetTimeLatest})
		}
		if uint(evict.Len()) > r.params.EvictionBatchSize {
			heap.Pop(evict)
		}
	}
	r.mu.Unlock()
	evictKeys := make([]string, evict.Len())
	for i, kgt := range *evict {
		evictKeys[i] = kgt.key
	}
	return evictKeys, nil
}

func (r *memoryAccessRecorder) Evict(keys []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range keys {
		if _, in := r.records[key]; !in {
			return ErrMissingValue
		}
		delete(r.records, key)
	}
	return nil
}

func (r *memoryAccessRecorder) update(key string, update *AccessRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, in := r.records[key]
	if !in {
		return ErrMissingValue
	}
	updateAccessRecord(existing, update)
	r.records[key] = existing
	return nil
}

type keyGetTime struct {
	key     string
	getTime time.Time
}

// keyGetTimes is a max-heap of keyGetTime objects
type keyGetTimes []keyGetTime

func (kgt keyGetTimes) Len() int {
	return len(kgt)
}

func (kgt keyGetTimes) Less(i, j int) bool {
	// After instead of Before turns min-heap into max-heap
	return kgt[i].getTime.After(kgt[j].getTime)
}

func (kgt keyGetTimes) Swap(i, j int) {
	kgt[i], kgt[j] = kgt[j], kgt[i]
}

func (kgt *keyGetTimes) Push(x interface{}) {
	*kgt = append(*kgt, x.(keyGetTime))
}

func (kgt *keyGetTimes) Pop() interface{} {
	old := *kgt
	n := len(old)
	x := old[n-1]
	*kgt = old[0 : n-1]
	return x
}

func (kgt keyGetTimes) Peak() keyGetTime {
	return kgt[0]
}
