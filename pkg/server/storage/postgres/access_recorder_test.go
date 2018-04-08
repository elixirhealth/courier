package postgres

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"

	"github.com/drausin/libri/libri/common/logging"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/elixirhealth/courier/pkg/server/storage/postgres/migrations"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/mattes/migrate/source/go-bindata"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var (
	setUpPostgresTest func(t *testing.T) (dbURL string, tearDown func() error)

	errTest = errors.New("test error")
)

func TestMain(m *testing.M) {
	dbURL, cleanup, err := bstorage.StartTestPostgres()
	if err != nil {
		if err2 := cleanup(); err2 != nil {
			log.Fatal("test postgres cleanup error: " + err2.Error())
		}
		log.Fatal("test postgres start error: " + err.Error())
	}
	as := bindata.Resource(migrations.AssetNames(), migrations.Asset)
	logger := &bstorage.LogLogger{}
	setUpPostgresTest = func(t *testing.T) (string, func() error) {
		m := bstorage.NewBindataMigrator(dbURL, as, logger)
		if err := m.Up(); err != nil {
			t.Fatal(err)
		}
		return dbURL, m.Down
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := cleanup(); err != nil {
		log.Fatal(err.Error())
	}

	os.Exit(code)
}

func TestAccessRecorder_CachePut_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar, err := New(dbURL, params, lg)
	assert.Nil(t, err)

	key := []byte{1, 2, 3}
	err = ar.CachePut(key)
	assert.Nil(t, err)

	// second put should be no-op
	err = ar.CachePut(key)
	assert.Nil(t, err)

	row := ar.(*accessRecorder).qr.SelectQueryRowContext(context.Background(),
		psql.RunWith(ar.(*accessRecorder).dbCache).
			Select(count).
			From(fqAccessRecordTable).
			Where(andJoin(
				cachePutOccurredCol, // = True
				notNull(cachePutTimeMinCol),
				notNull(cachePutTimeMicroCol),
			)),
	)
	var nPut int
	err = row.Scan(&nPut)
	assert.Nil(t, err)
	assert.Equal(t, 1, nPut)
}

func TestAccessRecorder_LibriPut_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar, err := New(dbURL, params, lg)
	assert.Nil(t, err)
	key := []byte{1, 2, 3}

	// usually this comes first
	err = ar.CachePut(key)
	assert.Nil(t, err)

	err = ar.LibriPut(key)
	assert.Nil(t, err)

	// second put should be no-op
	err = ar.LibriPut(key)
	assert.Nil(t, err)

	row := ar.(*accessRecorder).qr.SelectQueryRowContext(context.Background(),
		psql.RunWith(ar.(*accessRecorder).dbCache).
			Select(count).
			From(fqAccessRecordTable).
			Where(andJoin(
				libriPutOccurredCol, // = True
				notNull(libriPutTimeMicroCol),
			)),
	)
	var nPut int
	err = row.Scan(&nPut)
	assert.Nil(t, err)
	assert.Equal(t, 1, nPut)
}

func TestAccessRecorder_CacheGet_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar, err := New(dbURL, params, lg)
	assert.Nil(t, err)
	key := []byte{1, 2, 3}

	// usually this comes first
	err = ar.CachePut(key)
	assert.Nil(t, err)

	// then this
	err = ar.LibriPut(key)
	assert.Nil(t, err)

	// then one or more of these, each of which should create a separate record
	err = ar.CacheGet(key)
	assert.Nil(t, err)

	err = ar.CacheGet(key)
	assert.Nil(t, err)

	row := ar.(*accessRecorder).qr.SelectQueryRowContext(context.Background(),
		psql.RunWith(ar.(*accessRecorder).dbCache).
			Select(count).
			From(fqAccessRecordTable).
			Where(andJoin(
				cacheGetOccurredCol, // = True
				notNull(cacheGetTimeMicroCol),
			)),
	)
	var nPut int
	err = row.Scan(&nPut)
	assert.Nil(t, err)
	assert.Equal(t, 2, nPut)
}

func TestAccessRecorder_CacheEvict(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar, err := New(dbURL, params, lg)
	assert.Nil(t, err)
	key1, key2 := []byte{1, 2, 3}, []byte{4, 5, 6}

	err = ar.CachePut(key1)
	assert.Nil(t, err)

	err = ar.CachePut(key2)
	assert.Nil(t, err)

	err = ar.LibriPut(key1) // in theory, key1 now evictable
	assert.Nil(t, err)

	err = ar.CacheEvict([][]byte{key1})
	assert.Nil(t, err)

	row := ar.(*accessRecorder).qr.SelectQueryRowContext(context.Background(),
		psql.RunWith(ar.(*accessRecorder).dbCache).
			Select(count).
			From(fqAccessRecordTable),
	)
	var nPut int
	err = row.Scan(&nPut)
	assert.Nil(t, err)
	assert.Equal(t, 1, nPut)
}

func TestAccessRecorder_GetNextEvictions(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	params.RecentWindowDays = 0
	params.LRUCacheSize = 1
	lg := logging.NewDevLogger(zap.DebugLevel)

	ar, err := New(dbURL, params, lg)
	assert.Nil(t, err)
	key1, key2, key3, key4 := []byte{1}, []byte{2}, []byte{3}, []byte{4}

	for _, key := range [][]byte{key1, key2, key3, key4} {
		err = ar.CachePut(key)
		assert.Nil(t, err)
	}

	// record libri puts for 3 of 4 keys
	for _, key := range [][]byte{key1, key2, key3} {
		err = ar.LibriPut(key)
		assert.Nil(t, err)
	}

	// since LRU cache size is 1, key1 and key2 should be evictable
	evictable, err := ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Equal(t, [][]byte{key1, key2}, evictable)
}
