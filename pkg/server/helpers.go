package server

import (
	"github.com/drausin/libri/libri/common/ecid"
	"github.com/elxirhealth/courier/pkg/cache"
)

func getClientID(config *Config) (ecid.ID, error) {
	if config.ClientIDFilepath != "" {
		return ecid.FromPrivateKeyFile(config.ClientIDFilepath)
	}
	return ecid.NewRandom(), nil
}

func getCache(config *Config) (cache.Cache, error) {
	cacheParams := cache.NewDefaultParameters()
	if config.CacheStorage == DataStore {
		dsCache, err := cache.NewDatastore(config.GCPProjectID, cacheParams)
		if err != nil {
			return nil, err
		}
		return dsCache, nil
	}

	// TODO (drausin) add default in-memory cache
	return nil, nil
}
