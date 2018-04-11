package postgres

import (
	"context"
	"math/rand"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/drausin/libri/libri/common/id"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/util"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// TestCache_PutGet_ok is an integration-y style test that his the DB.
func TestCache_PutGet_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	c, _, err := New(dbURL, params, lg)
	assert.Nil(t, err)

	valueSizes := []int{1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024}
	for _, valueSize := range valueSizes {
		value1 := util.RandBytes(rng, valueSize)
		key := util.RandBytes(rng, id.Length)

		exists, err := c.Put(key, value1)
		assert.Nil(t, err)
		assert.False(t, exists)

		// put again just to see no-op
		exists, err = c.Put(key, value1)
		assert.Nil(t, err)
		assert.True(t, exists)

		value2, err2 := c.Get(key)
		assert.Nil(t, err2)
		assert.Equal(t, value1, value2)

		cachePutCount := c.(*cache).qr.SelectQueryRowContext(context.Background(),
			psql.RunWith(c.(*cache).dbCache).
				Select(count).
				From(fqAccessRecordTable).
				Where(sq.Eq{keyCol: key, cachePutOccurredCol: true}),
		)
		var nPut int
		err = cachePutCount.Scan(&nPut)
		assert.Nil(t, err)
		assert.Equal(t, 1, nPut)

		cacheGetCount := c.(*cache).qr.SelectQueryRowContext(context.Background(),
			psql.RunWith(c.(*cache).dbCache).
				Select(count).
				From(fqAccessRecordTable).
				Where(sq.Eq{keyCol: key, cacheGetOccurredCol: true}),
		)
		var nGet int
		err = cacheGetCount.Scan(&nGet)
		assert.Nil(t, err)
		assert.Equal(t, 1, nPut)
	}
}

func TestCache_Put_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres

	okKey := util.RandBytes(rng, storage.KeySize)
	okValue := []byte{1, 2, 3}

	cases := map[string]struct {
		c        *cache
		key      []byte
		value    []byte
		expected error
	}{
		"bad key": {
			c: &cache{
				params: params,
				logger: lg,
			},
			key:      []byte{1, 2, 3},
			value:    okValue,
			expected: storage.ErrInvalidKeySize,
		},
		"bad value": {
			c: &cache{
				params: params,
				logger: lg,
			},
			key:      okKey,
			value:    util.RandBytes(rng, storage.MaxValueSize+1),
			expected: storage.ErrValueTooLarge,
		},
		"insert err": {
			c: &cache{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					insertErr: errTest,
				},
			},
			key:      okKey,
			value:    okValue,
			expected: errTest,
		},
		"cache put err": {
			c: &cache{
				params: params,
				logger: lg,
				qr: &fixedQuerier{
					insertResult: &fixedSQLResult{
						rowsAffected: 1,
					},
				},
				ar: &fixedAccessRecorder{
					cachePutErr: errTest,
				},
			},
			key:      okKey,
			value:    okValue,
			expected: errTest,
		},
	}

	for desc, c := range cases {
		exists, err := c.c.Put(c.key, c.value)
		assert.Equal(t, c.expected, err, desc)
		assert.False(t, exists)
	}
}

func TestCache_Get_err(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres

	c := &cache{
		params: params,
		logger: lg,
		qr: &fixedQuerier{
			selectRowResult: &fixedRowScanner{
				err: errTest,
			},
		},
	}
	value, err := c.Get([]byte{1})
	assert.Equal(t, errTest, err)
	assert.Nil(t, value)
}

func TestCache_EvictNext_ok(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres

	// no evictions
	c := &cache{
		params: params,
		qr:     &fixedQuerier{},
		ar:     &fixedAccessRecorder{nextEvictions: [][]byte{}},
		logger: lg,
	}

	err := c.EvictNext()
	assert.Nil(t, err)

	// some evictions
	evictions := [][]byte{{1}, {2}, {3}}
	c = &cache{
		params: params,
		qr: &fixedQuerier{
			deleteResult: &fixedSQLResult{
				rowsAffected: int64(len(evictions)),
			},
		},
		ar: &fixedAccessRecorder{
			nextEvictions: evictions,
		},
		logger: lg,
	}

	err = c.EvictNext()
	assert.Nil(t, err)
}

func TestCache_EvictNext_err(t *testing.T) {
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	evictions := [][]byte{{1}, {2}, {3}}

	cases := map[string]struct {
		c        *cache
		expected error
	}{
		"AR get next evictions err": {
			c: &cache{
				params: params,
				logger: lg,
				ar: &fixedAccessRecorder{
					nextEvictionsErr: errTest,
				},
			},
			expected: errTest,
		},
		"AR cache evict err": {
			c: &cache{
				params: params,
				logger: lg,
				ar: &fixedAccessRecorder{
					nextEvictions: evictions,
					cacheEvictErr: errTest,
				},
			},
			expected: errTest,
		},
		"delete err": {
			c: &cache{
				params: params,
				logger: lg,
				ar: &fixedAccessRecorder{
					nextEvictions: evictions,
				},
				qr: &fixedQuerier{
					deleteErr: errTest,
				},
			},
			expected: errTest,
		},
	}

	for desc, c := range cases {
		err := c.c.EvictNext()
		assert.Equal(t, c.expected, err, desc)
	}
}

type fixedAccessRecorder struct {
	cachePutErr      error
	cacheGetErr      error
	cacheEvictErr    error
	cacheEvictKeys   [][]byte
	libriPutErr      error
	nextEvictions    [][]byte
	nextEvictionsErr error
}

func (r *fixedAccessRecorder) CachePut(key []byte) error {
	return r.cachePutErr
}

func (r *fixedAccessRecorder) CacheGet(key []byte) error {
	return r.cacheGetErr
}

func (r *fixedAccessRecorder) CacheEvict(keys [][]byte) error {
	r.cacheEvictKeys = keys
	return r.cacheEvictErr
}

func (r *fixedAccessRecorder) LibriPut(key []byte) error {
	return r.libriPutErr
}

func (r *fixedAccessRecorder) GetNextEvictions() ([][]byte, error) {
	return r.nextEvictions, r.nextEvictionsErr
}

type fixedSQLResult struct {
	rowsAffected int64
}

func (f *fixedSQLResult) LastInsertId() (int64, error) {
	panic("implement me")
}

func (f *fixedSQLResult) RowsAffected() (int64, error) {
	return f.rowsAffected, nil
}
