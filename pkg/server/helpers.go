package server

import (
	"github.com/drausin/libri/libri/common/ecid"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
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

func getCache(config *Config, logger *zap.Logger) (storage.Cache, storage.AccessRecorder, error) {
	switch config.Storage.Type {
	case bstorage.DataStore:
		c, ar, err := storage.NewDatastore(config.GCPProjectID, config.Storage, logger)
		if err != nil {
			return nil, nil, err
		}
		return c, ar, nil
	case bstorage.Memory:
		c, ar := storage.NewMemory(config.Storage, logger)
		return c, ar, nil
	default:
		return nil, nil, ErrInvalidCacheStorageType
	}
}
