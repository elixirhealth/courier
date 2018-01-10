package server

import (
	"github.com/drausin/libri/libri/common/ecid"
	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/pkg/errors"
)

func getClientID(config *Config) (ecid.ID, error) {
	if config.ClientIDFilepath != "" {
		// TODO (drausin)
		return nil, errors.New("not implemented")
	}

	return ecid.NewRandom(), nil
}

func getCache(config *Config) (cache.Cache, error) {
	if config.CacheStorage == DataStore {
		dsCache, err := cache.NewDatastore(config.GCPProjectID)
		if err != nil {
			return nil, err
		}
		return dsCache, nil
	}

	// TODO (drausin) add default in-memory cache
	return nil, nil
}
