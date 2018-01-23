package server

import (
	"net"
	"testing"
	"time"

	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/stretchr/testify/assert"
)

func TestNewDefaultConfig(t *testing.T) {
	c := NewDefaultConfig()
	assert.NotEmpty(t, c.LibriGetTimeout)
	assert.NotEmpty(t, c.LibriPutTimeout)
	assert.NotEmpty(t, c.LibriPutQueueSize)
	assert.NotEmpty(t, c.Librarians)
	assert.NotEmpty(t, c.Cache)
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
	c1.WithDefaultCache()
	assert.Equal(t, c1.Cache.StorageType, c2.WithCache(nil).Cache.StorageType)
	assert.NotEqual(t,
		c1.Cache.StorageType,
		c3.WithCache(&cache.Parameters{StorageType: cache.DataStore}).Cache.StorageType,
	)
}

func TestConfig_WithBootstrapAddrs(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultLibrarianAddrs()
	assert.Equal(t, c1.Librarians, c2.WithLibrarianAddrs(nil).Librarians)
	c3Addr, err := net.ResolveTCPAddr("tcp4", "localhost:1234")
	assert.Nil(t, err)
	assert.NotEqual(t,
		c1.Librarians,
		c3.WithLibrarianAddrs([]*net.TCPAddr{c3Addr}).Librarians,
	)
}
