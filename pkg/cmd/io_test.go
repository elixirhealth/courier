package cmd

import (
	"fmt"
	"sync"
	"testing"

	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/elxirhealth/courier/pkg/server"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestTestIO(t *testing.T) {
	nDocs := uint(8)

	// start in-memory courier w/o librarians, so all libri puts will just be queued
	config := server.NewDefaultConfig()
	config.Cache.StorageType = cache.InMemory
	config.LibriPutQueueSize = nDocs * 2
	config.LogLevel = zapcore.DebugLevel
	config.ServerPort = 10200
	config.MetricsPort = 10201

	up := make(chan *server.Courier, 1)
	wg1 := new(sync.WaitGroup)
	wg1.Add(1)
	go func(wg2 *sync.WaitGroup) {
		defer wg2.Done()
		err := server.Start(config, up)
		assert.Nil(t, err)
	}(wg1)

	c := <-up
	viper.Set(couriersFlag, fmt.Sprintf("localhost:%d", config.ServerPort))
	viper.Set(nDocsFlag, nDocs)

	err := testIO()
	assert.Nil(t, err)

	c.StopServer()
	wg1.Wait()
}
