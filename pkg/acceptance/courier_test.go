// +build acceptance

package acceptance

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/logging"
	"github.com/drausin/libri/libri/common/parse"
	"github.com/drausin/libri/libri/librarian/api"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	lserver "github.com/drausin/libri/libri/librarian/server"
	"github.com/elixirhealth/catalog/pkg/catalogapi"
	catclient "github.com/elixirhealth/catalog/pkg/client"
	catserver "github.com/elixirhealth/catalog/pkg/server"
	"github.com/elixirhealth/courier/pkg/courierapi"
	cserver "github.com/elixirhealth/courier/pkg/server"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/elixirhealth/courier/pkg/server/storage/postgres/migrations"
	keyclient "github.com/elixirhealth/key/pkg/client"
	"github.com/elixirhealth/key/pkg/keyapi"
	keyserver "github.com/elixirhealth/key/pkg/server"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/util"
	"github.com/mattes/migrate/source/go-bindata"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type parameters struct {
	nLibrarians     int
	nCouriers       int
	nPuts           int
	nEntities       int
	nEntityPubKeys  int
	getTimeout      time.Duration
	putTimeout      time.Duration
	libriLogLevel   zapcore.Level
	catalogLogLevel zapcore.Level
	keyLogLevel     zapcore.Level
	courierLogLevel zapcore.Level
}

type state struct {
	dataDir           string
	couriers          []*cserver.Courier
	courierClients    []courierapi.CourierClient
	librarians        []*lserver.Librarian
	librarianAddrs    []*net.TCPAddr
	catalog           *catserver.Catalog
	catalogAddr       *net.TCPAddr
	catalogClient     catalogapi.CatalogClient
	key               *keyserver.Key
	keyAddr           *net.TCPAddr
	keyClient         keyapi.KeyClient
	datastoreEmulator *os.Process
	rng               *rand.Rand
	putDocs           []*api.Document
	entityPubKeys     map[string]map[keyapi.KeyType][][]byte
	dbURL             string
	tearDownPostgres  func() error
}

func TestAcceptance(t *testing.T) {
	params := &parameters{
		nLibrarians:     8,
		nCouriers:       3,
		nPuts:           32,
		nEntities:       8,
		nEntityPubKeys:  16,
		getTimeout:      1 * time.Second,
		putTimeout:      1 * time.Second,
		libriLogLevel:   zapcore.ErrorLevel,
		catalogLogLevel: zapcore.ErrorLevel,
		keyLogLevel:     zapcore.ErrorLevel,
		courierLogLevel: zapcore.InfoLevel,
	}
	st := setUp(t, params)

	addEntityPublicKeys(t, params, st)
	testPut(t, params, st)
	testGet(t, params, st)

	tearDown(t, st)
}

func testPut(t *testing.T, params *parameters, st *state) {
	putDocs := make([]*api.Document, params.nPuts)
	for c := 0; c < params.nPuts; c++ {
		var value *libriapi.Document
		if st.rng.Intn(2) == 0 {
			value, _ = api.NewTestDocument(st.rng)
		} else {
			env := libriapi.NewTestEnvelope(st.rng)
			env.AuthorPublicKey = randPubKey(keyapi.KeyType_AUTHOR, params, st)
			env.ReaderPublicKey = randPubKey(keyapi.KeyType_READER, params, st)
			value = &libriapi.Document{
				Contents: &libriapi.Document_Envelope{
					Envelope: env,
				},
			}
		}
		key, err := libriapi.GetKey(value)
		assert.Nil(t, err)

		putDocs[c] = value

		client := st.courierClients[st.rng.Int31n(int32(len(st.courierClients)))]
		rq := &courierapi.PutRequest{Key: key.Bytes(), Value: value}
		ctx, cancel := context.WithTimeout(context.Background(), params.putTimeout)
		_, err = client.Put(ctx, rq)
		cancel()

		assert.Nil(t, err)
		time.Sleep(250 * time.Millisecond)
	}
	st.putDocs = putDocs
}

func testGet(t *testing.T, params *parameters, st *state) {
	for c := 0; c < params.nPuts; c++ {
		value := st.putDocs[c]
		key, err := api.GetKey(value)
		assert.Nil(t, err)

		client := st.courierClients[st.rng.Int31n(int32(len(st.courierClients)))]
		rq := &courierapi.GetRequest{Key: key.Bytes()}
		ctx, cancel := context.WithTimeout(context.Background(), params.getTimeout)
		rp, err := client.Get(ctx, rq)
		cancel()

		assert.Nil(t, err)
		assert.Equal(t, value, rp.Value)

		switch ct := value.Contents.(type) {
		case *libriapi.Document_Envelope:
			// check to make sure catalog has envelope pub
			sRq := &catalogapi.SearchRequest{
				EntryKey: ct.Envelope.EntryKey,
				Limit:    1,
			}
			ctx, cancel = context.WithTimeout(context.Background(), params.getTimeout)
			sRp, err2 := st.catalogClient.Search(ctx, sRq)
			cancel()
			assert.Nil(t, err2)
			assert.Equal(t, 1, len(sRp.Result))

			// check that entity ID was enriched from key service
			assert.NotEmpty(t, sRp.Result[0].AuthorEntityId)
			assert.NotEmpty(t, sRp.Result[0].ReaderEntityId)
		}
	}
}

func setUp(t *testing.T, params *parameters) *state {
	dbURL, cleanup, err := bstorage.StartTestPostgres()
	if err != nil {
		t.Fatal(err)
	}

	st := &state{
		rng:              rand.New(rand.NewSource(0)),
		dbURL:            dbURL,
		tearDownPostgres: cleanup,
	}

	createAndStartLibrarians(params, st)
	createAndStartCatalog(params, st)
	createAndStartKey(params, st)
	createAndStartCouriers(params, st)

	return st
}

func addEntityPublicKeys(t *testing.T, params *parameters, st *state) {
	st.entityPubKeys = make(map[string]map[keyapi.KeyType][][]byte)
	for c := 0; c < params.nEntities; c++ {
		entityID := getEntityID(c)
		authorPKs := make([][]byte, params.nEntityPubKeys)
		readerPKs := make([][]byte, params.nEntityPubKeys)
		for i := range readerPKs {
			authorPKs[i] = util.RandBytes(st.rng, 33)
			readerPKs[i] = util.RandBytes(st.rng, 33)
		}
		st.entityPubKeys[entityID] = map[keyapi.KeyType][][]byte{
			keyapi.KeyType_AUTHOR: authorPKs,
			keyapi.KeyType_READER: readerPKs,
		}

		rq := &keyapi.AddPublicKeysRequest{
			EntityId:   entityID,
			KeyType:    keyapi.KeyType_AUTHOR,
			PublicKeys: authorPKs,
		}
		ctx, cancel := context.WithTimeout(context.Background(), params.putTimeout)
		_, err := st.keyClient.AddPublicKeys(ctx, rq)
		cancel()
		assert.Nil(t, err)

		rq = &keyapi.AddPublicKeysRequest{
			EntityId:   entityID,
			KeyType:    keyapi.KeyType_READER,
			PublicKeys: readerPKs,
		}
		ctx, cancel = context.WithTimeout(context.Background(), params.putTimeout)
		_, err = st.keyClient.AddPublicKeys(ctx, rq)
		cancel()
		assert.Nil(t, err)
	}
}

func randPubKey(kt keyapi.KeyType, params *parameters, st *state) []byte {
	entityID := getEntityID(st.rng.Intn(params.nEntities))
	i := st.rng.Intn(params.nEntityPubKeys)
	return st.entityPubKeys[entityID][kt][i]
}

func getEntityID(c int) string {
	return fmt.Sprintf("Entity-%d", c)
}

func createAndStartLibrarians(params *parameters, st *state) {
	dataDir, err := ioutil.TempDir("", "test-data-dir")
	errors.MaybePanic(err)
	configs, addrs := newLibrarianConfigs(dataDir, params.nLibrarians, params.libriLogLevel)
	logger := logging.NewDevLogger(params.libriLogLevel)

	librarians := make([]*lserver.Librarian, params.nLibrarians)
	up := make(chan *lserver.Librarian, 1)

	for c := 0; c < params.nLibrarians; c++ {
		go func() {
			err = lserver.Start(logger, configs[c], up)
			errors.MaybePanic(err)
		}()
		librarians[c] = <-up // wait for librarian to come up
	}

	st.dataDir = dataDir
	st.librarians = librarians
	st.librarianAddrs = addrs
}

func newLibrarianConfigs(dataDir string, nPeers int, logLevel zapcore.Level) (
	[]*lserver.Config, []*net.TCPAddr) {
	peerStartPort := 13000
	peerConfigs := make([]*lserver.Config, nPeers)
	peerAddrs := make([]*net.TCPAddr, nPeers)
	for c := 0; c < nPeers; c++ {
		localPort := peerStartPort + c
		peerConfigs[c] = newLibrarianConfig(dataDir, localPort, logLevel)
		peerConfigs[c].WithBootstrapAddrs([]*net.TCPAddr{peerConfigs[0].PublicAddr})
		peerAddrs[c] = peerConfigs[c].PublicAddr
	}

	return peerConfigs, peerAddrs
}

func newLibrarianConfig(dataDir string, port int, logLevel zapcore.Level) *lserver.Config {
	localAddr, err := parse.Addr("localhost", port)
	errors.MaybePanic(err) // should never happen
	peerDataDir := filepath.Join(dataDir, lserver.NameFromAddr(localAddr))

	return lserver.NewDefaultConfig().
		WithLocalPort(port).
		WithReportMetrics(false).
		WithDefaultPublicAddr().
		WithDefaultPublicName().
		WithDataDir(peerDataDir).
		WithDefaultDBDir().
		WithLogLevel(logLevel)
}

func createAndStartCouriers(params *parameters, st *state) {
	configs, addrs := newCourierConfigs(st, params)
	couriers := make([]*cserver.Courier, params.nCouriers)
	courierClients := make([]courierapi.CourierClient, params.nCouriers)
	up := make(chan *cserver.Courier, 1)

	for i := 0; i < params.nCouriers; i++ {
		go func() {
			err := cserver.Start(configs[i], up)
			errors.MaybePanic(err)
		}()

		// wait for courier to come up
		couriers[i] = <-up

		// set up catclient to it
		conn, err := grpc.Dial(addrs[i].String(), grpc.WithInsecure())
		errors.MaybePanic(err)
		courierClients[i] = courierapi.NewCourierClient(conn)
	}

	st.couriers = couriers
	st.courierClients = courierClients
}

func newCourierConfigs(st *state, params *parameters) (
	[]*cserver.Config, []*net.TCPAddr) {
	startPort := 10100
	configs := make([]*cserver.Config, params.nCouriers)
	addrs := make([]*net.TCPAddr, params.nCouriers)

	// set eviction params to ensure that evictions actually happen during test
	cacheParams := storage.NewDefaultParameters()
	cacheParams.Type = bstorage.Postgres
	cacheParams.LRUCacheSize = 4
	cacheParams.EvictionBatchSize = 4
	cacheParams.EvictionQueryTimeout = 5 * time.Second
	cacheParams.RecentWindowDays = -1 // i.e., everything is evictable
	cacheParams.EvictionPeriod = 5 * time.Second

	for i := 0; i < params.nCouriers; i++ {
		serverPort, metricsPort := startPort+i*10, startPort+i*10+1
		configs[i] = cserver.NewDefaultConfig().
			WithLibrarianAddrs(st.librarianAddrs).
			WithCatalogAddr(st.catalogAddr).
			WithKeyAddr(st.keyAddr).
			WithCache(cacheParams).
			WithDBUrl(st.dbURL)
		configs[i].WithServerPort(uint(serverPort)).
			WithMetricsPort(uint(metricsPort)).
			WithLogLevel(params.courierLogLevel)
		addrs[i] = &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPort}
	}
	return configs, addrs
}

func createAndStartCatalog(params *parameters, st *state) {
	config, addr := newCatalogConfig(params, st)
	up := make(chan *catserver.Catalog, 1)

	go func() {
		err := catserver.Start(config, up)
		errors.MaybePanic(err)
	}()

	// wait for catalog to come up
	st.catalog = <-up
	st.catalogAddr = addr
	cl, err := catclient.NewInsecure(addr.String())
	errors.MaybePanic(err)
	st.catalogClient = cl
}

func newCatalogConfig(params *parameters, st *state) (*catserver.Config, *net.TCPAddr) {
	startPort := 10200
	serverPort, metricsPort := startPort, startPort+1
	config := catserver.NewDefaultConfig().
		WithDBUrl(st.dbURL)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(metricsPort)).
		WithLogLevel(params.catalogLogLevel)
	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	return config, addr
}

func createAndStartKey(params *parameters, st *state) {
	config, addr := newKeyConfig(params)
	up := make(chan *keyserver.Key, 1)

	go func() {
		err := keyserver.Start(config, up)
		errors.MaybePanic(err)
	}()

	// wait for key to come up
	st.key = <-up
	st.keyAddr = addr
	cl, err := keyclient.NewInsecure(addr.String())
	errors.MaybePanic(err)
	st.keyClient = cl
}

func newKeyConfig(params *parameters) (*keyserver.Config, *net.TCPAddr) {
	startPort := 10300
	serverPort, metricsPort := startPort, startPort+1
	config := keyserver.NewDefaultConfig()
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(metricsPort)).
		WithLogLevel(params.keyLogLevel)
	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	return config, addr
}

func tearDown(t *testing.T, st *state) {

	// stop services
	for _, c := range st.couriers {
		go c.StopServer()
	}
	st.catalog.StopServer()
	st.key.StopServer()

	// stop librarians
	for _, p1 := range st.librarians {
		go func(p2 *lserver.Librarian) {
			// explicitly end subscriptions first and then sleep so that later librarians
			// don't crash b/c of flurry of ended subscriptions from earlier librarians
			p2.StopAuxRoutines()
			time.Sleep(5 * time.Second)
			err2 := p2.Close()
			errors.MaybePanic(err2)
		}(p1)
	}

	logger := &bstorage.ZapLogger{Logger: logging.NewDevInfoLogger()}
	m := bstorage.NewBindataMigrator(
		st.dbURL,
		bindata.Resource(migrations.AssetNames(), migrations.Asset),
		logger,
	)
	err := m.Down()
	assert.Nil(t, err)

	err = st.tearDownPostgres()
	assert.Nil(t, err)

	// remove data dir shared by all
	err = os.RemoveAll(st.dataDir)
	errors.MaybePanic(err)
}
