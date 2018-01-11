package server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewDefaultConfig(t *testing.T) {
	c := NewDefaultConfig()
	assert.NotEmpty(t, c.LibriGetTimeout)
	assert.NotEmpty(t, c.LibriPutTimeout)
	assert.NotEmpty(t, c.LibriPutQueueSize)
	assert.NotEmpty(t, c.CacheStorage)
	assert.NotEmpty(t, c.LibrarianAddrs)
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

func TestConfig_WithCacheStorage(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultCacheStorage()
	assert.Equal(t, c1.CacheStorage, c2.WithCacheStorage(Unspecified).CacheStorage)
	assert.NotEqual(t, c1.CacheStorage, c3.WithCacheStorage(DataStore).CacheStorage)
}

func TestConfig_WithBootstrapAddrs(t *testing.T) {
	c1, c2, c3 := &Config{}, &Config{}, &Config{}
	c1.WithDefaultLibrarianAddrs()
	assert.Equal(t, c1.LibrarianAddrs, c2.WithLibrarianAddrs(nil).LibrarianAddrs)
	c3Addr, err := net.ResolveTCPAddr("tcp4", "localhost:1234")
	assert.Nil(t, err)
	assert.NotEqual(t,
		c1.LibrarianAddrs,
		c3.WithLibrarianAddrs([]*net.TCPAddr{c3Addr}).LibrarianAddrs,
	)
}
