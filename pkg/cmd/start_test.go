package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/elxirhealth/courier/pkg/cache"
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
	viper.Set(cacheInMemoryStorageFlag, cacheInMemoryStorage)
	viper.Set(cacheDataStoreStorageFlag, cacheDataStoreStorage)
	viper.Set(cacheRecentWindowDaysFlag, cacheRecentWindowDays)
	viper.Set(cacheLRUSizeFlag, cacheLRUSize)
	viper.Set(cacheEvictionBatchSizeFlag, cacheEvictionBatchSize)
	viper.Set(cacheEvictionPeriodFlag, cacheEvictionPeriod)

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
	assert.Equal(t, cache.DataStore, c.Cache.StorageType)
	assert.Equal(t, cacheRecentWindowDays, c.Cache.RecentWindowDays)
	assert.Equal(t, cacheLRUSize, c.Cache.LRUCacheSize)
	assert.Equal(t, cacheEvictionBatchSize, c.Cache.EvictionBatchSize)
	assert.Equal(t, cacheEvictionPeriod, c.Cache.EvictionPeriod)
}

func TestGetCacheStorageType(t *testing.T) {
	viper.Set(cacheInMemoryStorageFlag, true)
	viper.Set(cacheDataStoreStorageFlag, false)
	st, err := getCacheStorageType()
	assert.Nil(t, err)
	assert.Equal(t, cache.InMemory, st)

	viper.Set(cacheInMemoryStorageFlag, false)
	viper.Set(cacheDataStoreStorageFlag, true)
	st, err = getCacheStorageType()
	assert.Nil(t, err)
	assert.Equal(t, cache.DataStore, st)

	viper.Set(cacheInMemoryStorageFlag, true)
	viper.Set(cacheDataStoreStorageFlag, true)
	st, err = getCacheStorageType()
	assert.Equal(t, errMultipleCacheStorageTypes, err)
	assert.Equal(t, cache.Unspecified, st)

	viper.Set(cacheInMemoryStorageFlag, false)
	viper.Set(cacheDataStoreStorageFlag, false)
	st, err = getCacheStorageType()
	assert.Equal(t, errNoCacheStorateType, err)
	assert.Equal(t, cache.Unspecified, st)
}
