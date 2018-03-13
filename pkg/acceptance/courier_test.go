// +build acceptance

package acceptance

import (
	"context"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/errors"
	"github.com/drausin/libri/libri/common/logging"
	"github.com/drausin/libri/libri/common/parse"
	"github.com/drausin/libri/libri/librarian/api"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	lserver "github.com/drausin/libri/libri/librarian/server"
	"github.com/elxirhealth/catalog/pkg/catalogapi"
	catclient "github.com/elxirhealth/catalog/pkg/client"
	catserver "github.com/elxirhealth/catalog/pkg/server"
	catstorage "github.com/elxirhealth/catalog/pkg/server/storage"
	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/elxirhealth/courier/pkg/courierapi"
	cserver "github.com/elxirhealth/courier/pkg/server"
	bstorage "github.com/elxirhealth/service-base/pkg/server/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

const (
	datastoreEmulatorHostEnv = "DATASTORE_EMULATOR_HOST"
)

type parameters struct {
	nLibrarians     int
	nCouriers       int
	nPuts           int
	getTimeout      time.Duration
	putTimeout      time.Duration
	gcpProjectID    string
	datastoreAddr   string
	libriLogLevel   zapcore.Level
	catalogLogLevel zapcore.Level
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
	datastoreEmulator *os.Process
	rng               *rand.Rand
	putDocs           []*api.Document
}

func TestAcceptance(t *testing.T) {
	params := &parameters{
		nLibrarians:     8,
		nCouriers:       3,
		nPuts:           32,
		getTimeout:      3 * time.Second,
		putTimeout:      3 * time.Second,
		gcpProjectID:    "dummy-courier-acceptance",
		datastoreAddr:   "localhost:2001",
		libriLogLevel:   zapcore.ErrorLevel,
		catalogLogLevel: zapcore.ErrorLevel,
		courierLogLevel: zapcore.InfoLevel,
	}
	st := setUp(params)

	// wait for datastore emulator to start
	time.Sleep(5 * time.Second)

	testPut(t, params, st)
	testGet(t, params, st)

	tearDown(st)
}

func testPut(t *testing.T, params *parameters, st *state) {
	putDocs := make([]*api.Document, params.nPuts)
	for c := 0; c < params.nPuts; c++ {
		var value *libriapi.Document
		if st.rng.Intn(2) == 0 {
			value, _ = api.NewTestDocument(st.rng)
		} else {
			value = &libriapi.Document{
				Contents: &libriapi.Document_Envelope{
					Envelope: libriapi.NewTestEnvelope(st.rng),
				},
			}
		}
		key, err := libriapi.GetKey(value)

		putDocs[c] = value

		client := st.courierClients[st.rng.Int31n(int32(len(st.courierClients)))]
		rq := &courierapi.PutRequest{Key: key.Bytes(), Value: value}
		ctx, cancel := context.WithTimeout(context.Background(), params.putTimeout)
		rp, err := client.Put(ctx, rq)
		cancel()

		assert.Nil(t, err)
		assert.Equal(t, courierapi.PutOperation_STORED, rp.Operation)
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
		}
	}
}

func setUp(params *parameters) *state {
	st := &state{rng: rand.New(rand.NewSource(0))}

	startDatastoreEmulator(params, st)
	createAndStartLibrarians(params, st)
	createAndStartCatalog(params, st)
	createAndStartCouriers(params, st)

	return st
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

func startDatastoreEmulator(params *parameters, st *state) {
	datastoreDataDir := path.Join(st.dataDir, "datastore")
	cmd := exec.Command("gcloud", "beta", "emulators", "datastore", "start",
		"--no-store-on-disk",
		"--host-port", params.datastoreAddr,
		"--project", params.gcpProjectID,
		"--data-dir", datastoreDataDir,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	errors.MaybePanic(err)
	st.datastoreEmulator = cmd.Process
	os.Setenv(datastoreEmulatorHostEnv, params.datastoreAddr)
}

func createAndStartCouriers(params *parameters, st *state) {
	configs, addrs := newCourierConfigs(st.librarianAddrs, st.catalogAddr, params)
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

func newCourierConfigs(libAddrs []*net.TCPAddr, catalogAddr *net.TCPAddr, params *parameters) (
	[]*cserver.Config, []*net.TCPAddr) {
	startPort := 10100
	configs := make([]*cserver.Config, params.nCouriers)
	addrs := make([]*net.TCPAddr, params.nCouriers)

	// set eviction params to ensure that evictions actually happen during test
	cacheParams := cache.NewDefaultParameters()
	cacheParams.Type = bstorage.DataStore
	cacheParams.LRUCacheSize = 4
	cacheParams.EvictionBatchSize = 4
	cacheParams.EvictionQueryTimeout = 5 * time.Second
	cacheParams.RecentWindowDays = -1 // i.e., everything is evictable
	cacheParams.EvictionPeriod = 5 * time.Second

	for i := 0; i < params.nCouriers; i++ {
		serverPort, metricsPort := startPort+i*10, startPort+i*10+1
		configs[i] = cserver.NewDefaultConfig().
			WithLibrarianAddrs(libAddrs).
			WithCatalogAddr(catalogAddr).
			WithCache(cacheParams).
			WithGCPProjectID(params.gcpProjectID)
		configs[i].WithServerPort(uint(serverPort)).
			WithMetricsPort(uint(metricsPort)).
			WithLogLevel(params.courierLogLevel)
		addrs[i] = &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPort}
	}
	return configs, addrs
}

func createAndStartCatalog(params *parameters, st *state) {
	config, addr := newCatalogConfig(params)
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

func newCatalogConfig(params *parameters) (*catserver.Config, *net.TCPAddr) {
	startPort := 10200
	storageParams := catstorage.NewDefaultParameters()
	storageParams.Type = bstorage.DataStore
	serverPort, metricsPort := startPort, startPort+1
	config := catserver.NewDefaultConfig().
		WithStorage(storageParams).
		WithGCPProjectID(params.gcpProjectID)
	config.WithServerPort(uint(serverPort)).
		WithMetricsPort(uint(metricsPort)).
		WithLogLevel(params.catalogLogLevel)
	addr := &net.TCPAddr{IP: net.ParseIP("localhost"), Port: int(serverPort)}
	return config, addr
}

func tearDown(st *state) {

	// stop couriers
	for _, c := range st.couriers {
		go c.StopServer()
	}

	// stop datastore emulator
	pgid, err := syscall.Getpgid(st.datastoreEmulator.Pid)
	errors.MaybePanic(err)
	err = syscall.Kill(-pgid, syscall.SIGKILL)
	errors.MaybePanic(err)

	// stop catalog
	st.catalog.StopServer()

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

	// remove data dir shared by all
	err = os.RemoveAll(st.dataDir)
	errors.MaybePanic(err)
}
