package server

import (
	"net"
	"testing"
	"time"

	"github.com/drausin/libri/libri/common/subscribe"
	"github.com/elixirhealth/courier/pkg/server/storage"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewDefaultConfig(t *testing.T) {
	c := NewDefaultConfig()
	assert.NotEmpty(t, c.LibriGetTimeout)
	assert.NotEmpty(t, c.LibriPutTimeout)
	assert.NotEmpty(t, c.LibriPutQueueSize)
	assert.NotEmpty(t, c.Storage)
	assert.NotEmpty(t, c.SubscribeTo)
}

func TestConfig_MarshalLogObject(t *testing.T) {
	oe := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
	c := NewDefaultConfig()
	err := c.MarshalLogObject(oe)
	assert.Nil(t, err)
}

func TestConfig_WithLibriGetTimeout(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultLibriGetTimeout()
	assert.Equal(t, c1.LibriGetTimeout, c2.WithLibriGetTimeout(0).LibriGetTimeout)
	assert.NotEqual(t, c1.LibriGetTimeout,
		c3.WithLibriGetTimeout(2*time.Second).LibriGetTimeout)
}

func TestConfig_WithLibriPutTimeout(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultLibriPutTimeout()
	assert.Equal(t, c1.LibriPutTimeout, c2.WithLibriPutTimeout(0).LibriPutTimeout)
	assert.NotEqual(t, c1.LibriPutTimeout,
		c3.WithLibriPutTimeout(2*time.Second).LibriPutTimeout)
}

func TestConfig_WithLibriPutQueueSize(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultLibriPutQueueSize()
	assert.Equal(t, c1.LibriPutQueueSize, c2.WithLibriPutQueueSize(0).LibriPutQueueSize)
	assert.NotEqual(t, c1.LibriPutQueueSize, c3.WithLibriPutQueueSize(2).LibriPutQueueSize)
}

func TestConfig_WithNLibriPutters(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultNLibriPutters()
	assert.Equal(t, c1.NLibriPutters, c2.WithNLibriPutters(0).NLibriPutters)
	assert.NotEqual(t, c1.NLibriPutters, c3.WithNLibriPutters(2).NLibriPutters)
}

func TestConfig_WithCatalogAddr(t *testing.T) {
	c := &Config{}
	addr, err := net.ResolveTCPAddr("tcp4", "localhost:1234")
	assert.Nil(t, err)
	assert.NotNil(t, c.WithCatalogAddr(addr).Catalog)
}

func TestConfig_WithCatalogPutTimeout(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultCatalogPutTimeout()
	assert.Equal(t, c1.CatalogPutTimeout, c2.WithCatalogPutTimeout(0).CatalogPutTimeout)
	assert.NotEqual(t, c1.CatalogPutTimeout,
		c3.WithCatalogPutTimeout(2*time.Second).CatalogPutTimeout)
}

func TestConfig_WithCatalogPutQueueSize(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultCatalogPutQueueSize()
	assert.Equal(t, c1.CatalogPutQueueSize, c2.WithCatalogPutQueueSize(0).CatalogPutQueueSize)
	assert.NotEqual(t, c1.CatalogPutQueueSize, c3.WithCatalogPutQueueSize(2).CatalogPutQueueSize)
}

func TestConfig_WithSubscribeTo(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultSubscribeTo()
	assert.Equal(t, c1.SubscribeTo, c2.WithSubscribeTo(nil).SubscribeTo)
	assert.NotEqual(t,
		c1.SubscribeTo,
		c3.WithSubscribeTo(&subscribe.ToParameters{}).SubscribeTo,
	)
}

func TestConfig_WithNCatalogPutters(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultNCatalogPutters()
	assert.Equal(t, c1.NCatalogPutters, c2.WithNCatalogPutters(0).NCatalogPutters)
	assert.NotEqual(t, c1.NCatalogPutters, c3.WithNCatalogPutters(2).NCatalogPutters)
}

func TestConfig_WithClientIDFilepath(t *testing.T) {
	c1 := &Config{}
	fp := "test filepath"
	c1.WithClientIDFilepath(fp)
	assert.Equal(t, fp, c1.ClientIDFilepath)
}

func TestConfig_WithGCPProjectID(t *testing.T) {
	c1 := &Config{}
	p := "project-ID"
	c1.WithGCPProjectID(p)
	assert.Equal(t, p, c1.GCPProjectID)
}

func TestConfig_WithCache(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultStorage()
	assert.Equal(t, c1.Storage.Type, c2.WithCache(nil).Storage.Type)
	assert.NotEqual(t,
		c1.Storage.Type,
		c3.WithCache(&storage.Parameters{Type: bstorage.DataStore}).Storage.Type,
	)
}

func TestConfig_WithLibrarianAddrs(t *testing.T) {
	c := &Config{}
	addr, err := net.ResolveTCPAddr("tcp4", "localhost:1234")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(c.WithLibrarianAddrs([]*net.TCPAddr{addr}).Librarians))
}

func TestConfig_WithKeyAddr(t *testing.T) {
	c := &Config{}
	addr, err := net.ResolveTCPAddr("tcp4", "localhost:1234")
	assert.Nil(t, err)
	assert.NotNil(t, c.WithKeyAddr(addr).Key)
}

func TestConfig_WithKeyGetTimeout(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultKeyGetTimeout()
	assert.Equal(t, c1.KeyGetTimeout, c2.WithKeyGetTimeout(0).KeyGetTimeout)
	assert.NotEqual(t, c1.KeyGetTimeout,
		c3.WithKeyGetTimeout(2*time.Second).KeyGetTimeout)
}

func TestConfig_WithDBUrl(t *testing.T) {
	c1 := &Config{}
	dbURL := "some DB URL"
	c1.WithDBUrl(dbURL)
	assert.Equal(t, dbURL, c1.DBUrl)
}
