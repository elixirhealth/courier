package cmd

import (
	"strings"
	"testing"
	"time"

	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestGetCourierConfig(t *testing.T) {
	serverPort := uint(1234)
	metricsPort := uint(5678)
	profilerPort := uint(9012)
	logLevel := zapcore.DebugLevel.String()
	profile := true
	libriTimeout := 10 * time.Second
	nLibrarianPutters := 8
	libriPutQueueSize := uint(32)
	gcpProjectID := "some project"
	librarians := []string{"127.0.0.1:1234", "127.0.0.1:4567"}
	cacheInMemoryStorage := false
	cacheDataStoreStorage := true
	cacheRecentWindowDays := 2
	cacheLRUSize := uint(1024)
	cacheEvictionBatchSize := uint(32)
	cacheEvictionPeriod := 32 * time.Minute
	catalog := "127.0.0.1:20100"
	nCatalogPutters := uint(12)
	catalogPutQueueSize := uint(48)
	catalogTimeout := 15 * time.Second
	key := "127.0.0.1:20200"
	keyTimeout := 16 * time.Second

	viper.Set(serverPortFlag, serverPort)
	viper.Set(metricsPortFlag, metricsPort)
	viper.Set(profilerPortFlag, profilerPort)
	viper.Set(logLevelFlag, logLevel)
	viper.Set(profileFlag, profile)
	viper.Set(libriTimeoutFlag, libriTimeout)
	viper.Set(nLibrarianPuttersFlag, nLibrarianPutters)
	viper.Set(libriPutQueueSizeFlag, libriPutQueueSize)
	viper.Set(gcpProjectIDFlag, gcpProjectID)
	viper.Set(librariansFlag, strings.Join(librarians, " "))
	viper.Set(cacheMemoryStorageFlag, cacheInMemoryStorage)
	viper.Set(cacheDataStoreStorageFlag, cacheDataStoreStorage)
	viper.Set(cacheRecentWindowDaysFlag, cacheRecentWindowDays)
	viper.Set(cacheLRUSizeFlag, cacheLRUSize)
	viper.Set(cacheEvictionBatchSizeFlag, cacheEvictionBatchSize)
	viper.Set(cacheEvictionPeriodFlag, cacheEvictionPeriod)
	viper.Set(catalogFlag, catalog)
	viper.Set(nCatalogPuttersFlag, nCatalogPutters)
	viper.Set(catalogPutQueueSizeFlag, catalogPutQueueSize)
	viper.Set(catalogTimeoutFlag, catalogTimeout)
	viper.Set(keyFlag, key)
	viper.Set(keyTimeoutFlag, keyTimeout)

	c, err := getCourierConfig()
	assert.Nil(t, err)
	assert.Equal(t, serverPort, c.ServerPort)
	assert.Equal(t, metricsPort, c.MetricsPort)
	assert.Equal(t, profilerPort, c.ProfilerPort)
	assert.Equal(t, logLevel, c.LogLevel.String())
	assert.Equal(t, profile, c.Profile)
	assert.Equal(t, libriTimeout, c.LibriPutTimeout)
	assert.Equal(t, libriTimeout, c.LibriGetTimeout)
	assert.Equal(t, libriPutQueueSize, c.LibriPutQueueSize)
	assert.Equal(t, gcpProjectID, c.GCPProjectID)
	for i, l := range c.Librarians {
		assert.Equal(t, librarians[i], l.String())
	}
	assert.Equal(t, bstorage.DataStore, c.Storage.Type)
	assert.Equal(t, cacheRecentWindowDays, c.Storage.RecentWindowDays)
	assert.Equal(t, cacheLRUSize, c.Storage.LRUCacheSize)
	assert.Equal(t, cacheEvictionBatchSize, c.Storage.EvictionBatchSize)
	assert.Equal(t, cacheEvictionPeriod, c.Storage.EvictionPeriod)
	assert.Equal(t, catalog, c.Catalog.String())
	assert.Equal(t, nCatalogPutters, c.NCatalogPutters)
	assert.Equal(t, catalogPutQueueSize, c.CatalogPutQueueSize)
	assert.Equal(t, catalogTimeout, c.CatalogPutTimeout)
	assert.Equal(t, key, c.Key.String())
	assert.Equal(t, keyTimeout, c.KeyGetTimeout)
}

func TestGetCacheStorageType(t *testing.T) {
	viper.Set(cacheMemoryStorageFlag, true)
	viper.Set(cacheDataStoreStorageFlag, false)
	st, err := getCacheStorageType()
	assert.Nil(t, err)
	assert.Equal(t, bstorage.Memory, st)

	viper.Set(cacheMemoryStorageFlag, false)
	viper.Set(cacheDataStoreStorageFlag, true)
	st, err = getCacheStorageType()
	assert.Nil(t, err)
	assert.Equal(t, bstorage.DataStore, st)

	viper.Set(cacheMemoryStorageFlag, true)
	viper.Set(cacheDataStoreStorageFlag, true)
	st, err = getCacheStorageType()
	assert.Equal(t, errMultipleCacheStorageTypes, err)
	assert.Equal(t, bstorage.Unspecified, st)

	viper.Set(cacheMemoryStorageFlag, false)
	viper.Set(cacheDataStoreStorageFlag, false)
	st, err = getCacheStorageType()
	assert.Equal(t, errNoCacheStorageType, err)
	assert.Equal(t, bstorage.Unspecified, st)
}
