package cmd

import (
	"fmt"
	"log"
	"net"
	"sync"
	"testing"

	"github.com/drausin/libri/libri/common/errors"
	catserver "github.com/elxirhealth/catalog/pkg/server"
	"github.com/elxirhealth/courier/pkg/server"
	keyserver "github.com/elxirhealth/key/pkg/server"
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

	// start in-memory key
	keyStartPort := uint(10300)
	keyConfig := keyserver.NewDefaultConfig()
	keyConfig.WithServerPort(keyStartPort).
		WithMetricsPort(keyStartPort + 1).
		WithLogLevel(zapcore.DebugLevel)
	keyUp := make(chan *keyserver.Key, 1)
	go func() {
		err := keyserver.Start(keyConfig, keyUp)
		errors.MaybePanic(err)
	}()
	// key := <-keyUp  // see comment re stopping key at bottom
	<-keyUp

	// start in-memory courier w/o librarians, so all libri puts will just be queued
	libAddrs := []*net.TCPAddr{{IP: net.ParseIP("localhost"), Port: 20100}}
	config := server.NewDefaultConfig().
		WithLibrarianAddrs(libAddrs).
		WithCatalogAddr(&net.TCPAddr{
			IP:   net.ParseIP("localhost"),
			Port: int(catalogStartPort)},
		).
		WithKeyAddr(&net.TCPAddr{
			IP:   net.ParseIP("localhost"),
			Port: int(keyStartPort)},
		)
	config.SubscribeTo.NSubscriptions = 0
	config.LibriPutQueueSize = nDocs * 2
	config.LogLevel = zapcore.DebugLevel
	config.ServerPort = 10400
	config.MetricsPort = 10401

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
	log.Println("stopped courier")
	catalog.StopServer()
	log.Println("stopped catalog")

	// for some reason, this hangs, so omitting until can actually figure out what's going on
	// key.StopServer()
}
