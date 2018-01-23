package cmd

import (
	"testing"
	"time"

	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestGetCourierConfig(t *testing.T) {
	serverPort := 1234
	metricsPort := 5678
	profilerPort := 9012
	logLevel := zapcore.DebugLevel.String()
	profile := true
	libriTimeout := 10 * time.Second
	nLibrarianPutters := 8
	libriPutQueueSize := 32
	gcpProjectID := "some project"

	viper.Set()
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
