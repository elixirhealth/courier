package cmd

import (
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/drausin/libri/libri/common/errors"
	catserver "github.com/elxirhealth/catalog/pkg/server"
	"github.com/elxirhealth/courier/pkg/server"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestTestIO(t *testing.T) {
	nDocs := uint(8)

	// start in-memory catalog
	catalogStartPort := uint(10200)
	catalogConfig := catserver.NewDefaultConfig()
	catalogConfig.WithServerPort(catalogStartPort).
		WithMetricsPort(catalogStartPort + 1)
	catalogUp := make(chan *catserver.Catalog, 1)
	go func() {
		err := catserver.Start(catalogConfig, catalogUp)
		errors.MaybePanic(err)
	}()
	catalog := <-catalogUp

	// start in-memory courier w/o librarians, so all libri puts will just be queued
	libAddrs := []*net.TCPAddr{{IP: net.ParseIP("localhost"), Port: 20100}}
	config := server.NewDefaultConfig().
		WithLibrarianAddrs(libAddrs).
		WithCatalogAddr(&net.TCPAddr{
			IP:   net.ParseIP("localhost"),
			Port: int(catalogStartPort)},
		)
	config.SubscribeTo.NSubscriptions = 0
	config.LibriPutQueueSize = nDocs * 2
	config.LogLevel = zapcore.InfoLevel
	config.ServerPort = 10300
	config.MetricsPort = 10301

	up := make(chan *server.Courier, 1)
	wg1 := new(sync.WaitGroup)
	wg1.Add(1)
	go func(wg2 *sync.WaitGroup) {
		defer wg2.Done()
		err := server.Start(config, up)
		errors.MaybePanic(err)
	}(wg1)

	c := <-up
	viper.Set(couriersFlag, fmt.Sprintf("localhost:%d", config.ServerPort))
	viper.Set(nDocsFlag, nDocs)

	err := testIO()
	assert.Nil(t, err)

	c.StopServer()
	wg1.Wait()
	catalog.StopServer()
}
