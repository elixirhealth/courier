package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	sq "github.com/Masterminds/squirrel"
	errors2 "github.com/drausin/libri/libri/common/errors"
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

// Most of the happy/ok path is tested through actual queries to Postgres DB. While this kind of
// integration-y testing is less desirable than unit testing, much of the AccessRecorder logic
// lies in the SQL queries and DB structure and indices, which would be harder and more confusing
// to mock than just to use the real thing. Error path tests use a combo of mocks and DB.

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

	ar, err := newAccessRecorder(dbURL, params, lg)
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

	ar, err := newAccessRecorder(dbURL, params, lg)
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

	ar, err := newAccessRecorder(dbURL, params, lg)
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

func TestAccessRecorder_PutGet_err(t *testing.T) {
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar := &accessRecorder{
		params: params,
		logger: lg,
		qr:     &fixedQuerier{insertErr: errTest},
	}
	key := []byte{1, 2, 3}

	err := ar.CachePut(key)
	assert.Equal(t, errTest, err)

	err = ar.LibriPut(key)
	assert.Equal(t, errTest, err)

	err = ar.CacheGet(key)
	assert.Equal(t, errTest, err)
}

func TestAccessRecorder_CacheEvict_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar, err := newAccessRecorder(dbURL, params, lg)
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

func TestAccessRecorder_CacheEvict_err(t *testing.T) {
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar := &accessRecorder{
		params: params,
		logger: lg,
		qr:     &fixedQuerier{deleteErr: errTest},
	}

	err := ar.CacheEvict([][]byte{{1}, {2}})
	assert.Equal(t, errTest, err)
}

func TestAccessRecorder_GetNextEvictions_ok(t *testing.T) {
	dbURL, tearDown := setUpPostgresTest(t)
	defer func() {
		err := tearDown()
		assert.Nil(t, err)
	}()

	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	params.RecentWindowDays = 0
	params.LRUCacheSize = 1
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	ar, err := newAccessRecorder(dbURL, params, lg)
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

	// evict key1 & key2
	err = ar.CacheEvict(evictable)
	assert.Nil(t, err)

	// no more evictable keys, since LRU cache is 1
	evictable, err = ar.GetNextEvictions()
	assert.Nil(t, err)
	assert.Empty(t, evictable)
}

func TestAccessRecorder_GetNextEvictions_err(t *testing.T) {
	params := storage.NewDefaultParameters()
	params.Type = bstorage.Postgres
	params.LRUCacheSize = 1
	lg := zap.NewNop() // logging.NewDevLogger(zap.DebugLevel)

	cases := map[string]*accessRecorder{
		"evictable count scan err": {
			params: params,
			logger: lg,
			qr: &fixedQuerier{
				selectRowResult: &fixedRowScanner{err: errTest},
			},
		},
		"evictable keys query err": {
			params: params,
			logger: lg,
			qr: &fixedQuerier{
				selectRowResult: &fixedRowScanner{val: 2},
				selectErr:       errTest,
			},
		},
		"evictable keys scan err": {
			params: params,
			logger: lg,
			qr: &fixedQuerier{
				selectRowResult: &fixedRowScanner{val: 2},
				selectResult: &fixedQueryRows{
					next:    true,
					scanErr: errTest,
				},
			},
		},
		"evictable keys err err": {
			params: params,
			logger: lg,
			qr: &fixedQuerier{
				selectRowResult: &fixedRowScanner{val: 2},
				selectResult: &fixedQueryRows{
					next:   false,
					errErr: errTest,
				},
			},
		},
		"evictable keys close err": {
			params: params,
			logger: lg,
			qr: &fixedQuerier{
				selectRowResult: &fixedRowScanner{val: 2},
				selectResult: &fixedQueryRows{
					next:     false,
					closeErr: errTest,
				},
			},
		},
	}

	for desc, c := range cases {
		evictions, err := c.GetNextEvictions()
		assert.Equal(t, errTest, err, desc)
		assert.Nil(t, evictions)
	}
}

func newAccessRecorder(
	dbURL string, params *storage.Parameters, logger *zap.Logger,
) (storage.AccessRecorder, error) {
	db, err := sql.Open("postgres", dbURL)
	errors2.MaybePanic(err)
	return &accessRecorder{
		params:  params,
		db:      db,
		dbCache: sq.NewStmtCacher(db),
		qr:      bstorage.NewQuerier(),
		logger:  logger,
	}, nil
}

func notNull(expr string) string {
	return fmt.Sprintf("%s IS NOT NULL", expr)
}

type fixedQuerier struct {
	selectResult    bstorage.QueryRows
	selectErr       error
	selectRowResult sq.RowScanner
	insertResult    sql.Result
	insertErr       error
	deleteResult    sql.Result
	deleteErr       error
}

func (f *fixedQuerier) SelectQueryContext(
	ctx context.Context, b sq.SelectBuilder,
) (bstorage.QueryRows, error) {
	return f.selectResult, f.selectErr
}

func (f *fixedQuerier) SelectQueryRowContext(
	ctx context.Context, b sq.SelectBuilder,
) sq.RowScanner {
	return f.selectRowResult
}

func (f *fixedQuerier) InsertExecContext(
	ctx context.Context, b sq.InsertBuilder,
) (sql.Result, error) {
	return f.insertResult, f.insertErr
}

func (f *fixedQuerier) UpdateExecContext(
	ctx context.Context, b sq.UpdateBuilder,
) (sql.Result, error) {
	panic("implement me")
}

func (f *fixedQuerier) DeleteExecContext(
	ctx context.Context, b sq.DeleteBuilder,
) (sql.Result, error) {
	return f.deleteResult, f.deleteErr
}

type fixedRowScanner struct {
	val int
	err error
}

func (f *fixedRowScanner) Scan(dests ...interface{}) error {
	if f.err != nil {
		return f.err
	}
	srcRef := reflect.ValueOf(f.val)
	vp := reflect.ValueOf(dests[0])
	vp.Elem().Set(srcRef)
	return nil
}

type fixedQueryRows struct {
	next     bool
	scanVal  interface{}
	scanErr  error
	errErr   error
	closeErr error
}

func (f *fixedQueryRows) Close() error {
	return f.closeErr
}

func (f *fixedQueryRows) Err() error {
	return f.errErr
}

func (f *fixedQueryRows) Next() bool {
	return f.next
}

func (f *fixedQueryRows) Scan(dest ...interface{}) error {
	if f.scanErr != nil {
		return f.scanErr
	}

	// assume just single dest
	dest[0] = &f.scanVal
	return nil
}
