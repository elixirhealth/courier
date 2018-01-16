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

func getCache(config *Config) (cache.Cache, cache.AccessRecorder, error) {
	if config.Cache.StorageType == cache.DataStore {
		dsCache, ar, err := cache.NewDatastore(config.GCPProjectID, config.Cache)
		if err != nil {
			return nil, nil, err
		}
		return dsCache, ar, nil
	}

	// TODO (drausin) add default in-memory Cache
	return nil, nil, nil
}
