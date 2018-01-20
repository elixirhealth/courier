package server

import (
	"github.com/drausin/libri/libri/common/ecid"
	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

var (
	// ErrInvalidCacheStorageType indicates when a storage type is not expected.
	ErrInvalidCacheStorageType = errors.New("invalid cache storage type")
)

func getClientID(config *Config) (ecid.ID, error) {
	if config.ClientIDFilepath != "" {
		return ecid.FromPrivateKeyFile(config.ClientIDFilepath)
	}
	return ecid.NewRandom(), nil
}

func getCache(config *Config, logger *zap.Logger) (cache.Cache, cache.AccessRecorder, error) {
	switch config.Cache.StorageType {
	case cache.DataStore:
		c, ar, err := cache.NewDatastore(config.GCPProjectID, config.Cache, logger)
		if err != nil {
			return nil, nil, err
		}
		return c, ar, nil
	case cache.InMemory:
		c, ar := cache.NewMemory(config.Cache, logger)
		return c, ar, nil
	default:
		return nil, nil, ErrInvalidCacheStorageType
	}
}
