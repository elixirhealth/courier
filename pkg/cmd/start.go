package cmd

import (
	"log"
	"net"
	"os"

	cerrors "github.com/drausin/libri/libri/common/errors"
	lserver "github.com/drausin/libri/libri/common/logging"
	"github.com/drausin/libri/libri/common/parse"
	"github.com/elixirhealth/courier/pkg/server"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bserver "github.com/elixirhealth/service-base/pkg/server"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	serverPortFlag             = "serverPort"
	metricsPortFlag            = "metricsPort"
	profilerPortFlag           = "profilerPort"
	profileFlag                = "profile"
	libriTimeoutFlag           = "libriTimeout"
	nLibrarianPuttersFlag      = "nLibrarianPutters"
	libriPutQueueSizeFlag      = "libriPutQueueSize"
	dbURLFlag                  = "dbURL"
	dbPasswordFlag             = "dbPassword"
	librariansFlag             = "librarians"
	storageMemoryFlag          = "storageMemory"
	storagePostgresFlag        = "storagePostgres"
	cacheRecentWindowDaysFlag  = "cacheRecentWindowDays"
	cacheLRUSizeFlag           = "cacheLRUSize"
	cacheEvictionBatchSizeFlag = "cacheEvictionBatchSize"
	cacheEvictionPeriodFlag    = "cacheEvictionPeriod"
	catalogFlag                = "catalog"
	nCatalogPuttersFlag        = "nCatalogPutters"
	catalogPutQueueSizeFlag    = "catalogPutQueueSize"
	catalogTimeoutFlag         = "catalogTimeout"
	keyFlag                    = "key"
	keyTimeoutFlag             = "keyTimeout"
)

var (
	errMissingLibrarians         = errors.New("missing librarian addresses")
	errMissingCatalog            = errors.New("missing catalog address")
	errMissingKey                = errors.New("missing key address")
	errMultipleCacheStorageTypes = errors.New("multiple cache storage types specified")
	errNoStorageType             = errors.New("no cache storage type specified")
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "start a courier server",
	Run: func(cmd *cobra.Command, args []string) {
		writeBanner(os.Stdout)
		config, err := getCourierConfig()
		if err != nil {
			log.Fatal(err)
		}
		if err = server.Start(config, make(chan *server.Courier, 1)); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(startCmd)

	startCmd.Flags().Uint(serverPortFlag, bserver.DefaultServerPort,
		"port for the main service")
	startCmd.Flags().Uint(metricsPortFlag, bserver.DefaultMetricsPort,
		"port for Prometheus metrics")
	startCmd.Flags().Uint(profilerPortFlag, bserver.DefaultProfilerPort,
		"port for profiler endpoints (when enabled)")
	startCmd.Flags().Bool(profileFlag, bserver.DefaultProfile,
		"whether to enable profiler")
	startCmd.Flags().Duration(libriTimeoutFlag, server.DefaultLibriGetTimeout,
		"timeout for libri Put & Get requests")
	startCmd.Flags().Uint(nLibrarianPuttersFlag, server.DefaultNLibriPutters,
		"number of workers Putting documents into libri")
	startCmd.Flags().Uint(libriPutQueueSizeFlag, server.DefaultLibriPutQueueSize,
		"size of the queue for documents to Put into libri")
	startCmd.Flags().String(dbURLFlag, "", "Postgres DB URL, including username")
	startCmd.Flags().String(dbPasswordFlag, "", "DB user's password")
	startCmd.Flags().StringSlice(librariansFlag, []string{},
		"space-separated libri librarian addresses")
	startCmd.Flags().Bool(storageMemoryFlag, false,
		"cache uses in-memory storage")
	startCmd.Flags().Bool(storagePostgresFlag, false,
		"cache uses GCP DataStore storage")
	startCmd.Flags().Int(cacheRecentWindowDaysFlag, storage.DefaultRecentWindowDays,
		"recent past window in which cache entries are never evicted")
	startCmd.Flags().Uint(cacheLRUSizeFlag, storage.DefaultLRUCacheSize,
		"size of LRU cache before recent window")
	startCmd.Flags().Uint(cacheEvictionBatchSizeFlag, storage.DefaultEvictionBatchSize,
		"size of each batch of evictions")
	startCmd.Flags().Duration(cacheEvictionPeriodFlag, storage.DefaultEvictionPeriod,
		"period between evictions")
	startCmd.Flags().String(catalogFlag, "",
		"catalog service address")
	startCmd.Flags().Uint(nCatalogPuttersFlag, server.DefaultNCatalogPutters,
		"number of workers putting publications into the catalog")
	startCmd.Flags().Uint(catalogPutQueueSizeFlag, server.DefaultCatalogPutQueueSize,
		"size of the queue for publications to put into the catalog")
	startCmd.Flags().Duration(catalogTimeoutFlag, server.DefaultCatalogPutTimeout,
		"timeout for catalog Put requests")
	startCmd.Flags().String(keyFlag, "",
		"key service address")
	startCmd.Flags().Duration(keyTimeoutFlag, server.DefaultKeyGetTimeout,
		"timeout for key Get requests")

	// bind viper flags
	viper.SetEnvPrefix(envVarPrefix) // look for env vars with "COURIER_" prefix
	viper.AutomaticEnv()             // read in environment variables that match
	cerrors.MaybePanic(viper.BindPFlags(startCmd.Flags()))
}

func getCourierConfig() (*server.Config, error) {
	librarianStrAddrs := viper.GetStringSlice(librariansFlag)
	if len(librarianStrAddrs) == 0 {
		return nil, errMissingLibrarians
	}
	librarianAddrs, err := parse.Addrs(librarianStrAddrs)
	if err != nil {
		return nil, err
	}
	catalogStrAddr := viper.GetString(catalogFlag)
	if catalogStrAddr == "" {
		return nil, errMissingCatalog
	}
	catalogAddr, err := net.ResolveTCPAddr("tcp4", catalogStrAddr)
	if err != nil {
		return nil, err
	}
	keyStrAddr := viper.GetString(keyFlag)
	if keyStrAddr == "" {
		return nil, errMissingKey
	}
	keyAddr, err := net.ResolveTCPAddr("tcp4", keyStrAddr)
	if err != nil {
		return nil, err
	}
	cacheStorage, err := getCacheStorageType()
	if err != nil {
		return nil, err
	}

	cacheConfig := storage.NewDefaultParameters()
	cacheConfig.Type = cacheStorage
	cacheConfig.RecentWindowDays = viper.GetInt(cacheRecentWindowDaysFlag)
	cacheConfig.LRUCacheSize = uint(viper.GetInt(cacheLRUSizeFlag))
	cacheConfig.EvictionBatchSize = uint(viper.GetInt(cacheEvictionBatchSizeFlag))
	cacheConfig.EvictionPeriod = viper.GetDuration(cacheEvictionPeriodFlag)

	c := server.NewDefaultConfig()
	c.WithServerPort(uint(viper.GetInt(serverPortFlag))).
		WithMetricsPort(uint(viper.GetInt(metricsPortFlag))).
		WithProfilerPort(uint(viper.GetInt(profilerPortFlag))).
		WithLogLevel(lserver.GetLogLevel(viper.GetString(logLevelFlag))).
		WithProfile(viper.GetBool(profileFlag))
	c.WithLibriGetTimeout(viper.GetDuration(libriTimeoutFlag)).
		WithLibriPutTimeout(viper.GetDuration(libriTimeoutFlag)).
		WithNLibriPutters(uint(viper.GetInt(nLibrarianPuttersFlag))).
		WithLibriPutQueueSize(uint(viper.GetInt(libriPutQueueSizeFlag))).
		WithDBUrl(viper.GetString(dbURLFlag)).
		WithLibrarianAddrs(librarianAddrs).
		WithCache(cacheConfig).
		WithCatalogAddr(catalogAddr).
		WithNCatalogPutters(uint(viper.GetInt(nCatalogPuttersFlag))).
		WithCatalogPutQueueSize(uint(viper.GetInt(catalogPutQueueSizeFlag))).
		WithCatalogPutTimeout(viper.GetDuration(catalogTimeoutFlag)).
		WithKeyAddr(keyAddr).
		WithKeyGetTimeout(viper.GetDuration(keyTimeoutFlag))

	lg := lserver.NewDevLogger(c.LogLevel)
	lg.Info("successfully parsed config", zap.Object("config", c))

	return c, nil
}

func getCacheStorageType() (bstorage.Type, error) {
	if viper.GetBool(storageMemoryFlag) && viper.GetBool(storagePostgresFlag) {
		return bstorage.Unspecified, errMultipleCacheStorageTypes
	}
	if viper.GetBool(storageMemoryFlag) {
		return bstorage.Memory, nil
	}
	if viper.GetBool(storagePostgresFlag) {
		return bstorage.DataStore, nil
	}
	return bstorage.Unspecified, errNoStorageType
}
