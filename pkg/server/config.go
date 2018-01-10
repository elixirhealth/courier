package server

import (
	"fmt"
	"net"
	"time"

	"github.com/elxirhealth/courier/pkg/util"
)

// CacheStorageType indicates how the cache is stored.
type CacheStorageType int

const (
	// Unspecified indicates when the storage type is not specified (and thus should take the
	// default value).
	Unspecified CacheStorageType = iota

	// InMemory indicates an ephemeral, in-memory (and thus not highly available) cache. This
	// storage layer should generally only be used during testing and not in production.
	InMemory

	// DataStore indicates a (highly available) cache backed by GCP DataStore.
	DataStore
)

const (
	// DefaultLibriGetTimeout is the default timeout for libri Get requests.
	DefaultLibriGetTimeout = 5 * time.Second

	// DefaultLibriPutTimeout is the default timeout for libri Put requests.
	DefaultLibriPutTimeout = 5 * time.Second

	// DefaultLibriPutQueueSize is the default size of the libri Put queue.
	DefaultLibriPutQueueSize = 64

	// DefaultCacheStorage is the default storage type for the cache.
	DefaultCacheStorage = InMemory

	// DefaultLibrarianPort is the default port of the librarian server.
	DefaultLibrarianPort = 20100

	// DefaultLibrarianIP is the default IP of of the librarian server.
	DefaultLibrarianIP = "localhost"
)

// Config is the config for a courier instance.
type Config struct {
	LibriGetTimeout   time.Duration
	LibriPutTimeout   time.Duration
	LibriPutQueueSize uint
	ClientIDFilepath  string
	CacheStorage      CacheStorageType
	GCPProjectID      string
	LibrarianAddrs    []*net.TCPAddr
}

// NewDefaultConfig create a new config instance with default values.
func NewDefaultConfig() *Config {
	config := &Config{}
	return config.
		WithDefaultLibriGetTimeout().
		WithDefaultLibriPutTimeout().
		WithDefaultLibriPutQueueSize().
		WithDefaultCacheStorage().
		WithDefaultLibrarianAddrs()
}

// WithLibriGetTimeout sets the libri Get request timeout to the given value or to the default
// if it is zero-valued.
func (c *Config) WithLibriGetTimeout(t time.Duration) *Config {
	if t == 0 {
		return c.WithDefaultLibriGetTimeout()
	}
	c.LibriGetTimeout = t
	return c
}

// WithDefaultLibriGetTimeout sets the libri Get request timeout to the default value.
func (c *Config) WithDefaultLibriGetTimeout() *Config {
	c.LibriGetTimeout = DefaultLibriGetTimeout
	return c
}

// WithLibriPutTimeout sets the libri Put request timeout to the given value or to the default
// if it is zero-valued.
func (c *Config) WithLibriPutTimeout(t time.Duration) *Config {
	if t == 0 {
		return c.WithDefaultLibriPutTimeout()
	}
	c.LibriGetTimeout = t
	return c
}

// WithDefaultLibriPutTimeout sets the libri Put request timeout to the default value.
func (c *Config) WithDefaultLibriPutTimeout() *Config {
	c.LibriPutTimeout = DefaultLibriPutTimeout
	return c
}

// WithLibriPutQueueSize sets the libri Put queue size to the given value or to the default if it
// is zero-valued.
func (c *Config) WithLibriPutQueueSize(s uint) *Config {
	if s == 0 {
		return c.WithDefaultLibriPutQueueSize()
	}
	c.LibriPutQueueSize = s
	return c
}

// WithDefaultLibriPutQueueSize sets the libri Put queue size to the default value.
func (c *Config) WithDefaultLibriPutQueueSize() *Config {
	c.LibriPutQueueSize = DefaultLibriPutQueueSize
	return c
}

// WithClientIDFilepath sets the file path for the local *.der file containing the clientID private
// key.
func (c *Config) WithClientIDFilepath(fp string) *Config {
	if fp == "" {
		return c
	}
	c.ClientIDFilepath = fp
	return c
}

// WithCacheStorage sets the cache storage type to the given value or to the default if it is
// unspecified.
func (c *Config) WithCacheStorage(cst CacheStorageType) *Config {
	if cst == Unspecified {
		return c.WithDefaultCacheStorage()
	}
	c.CacheStorage = cst
	return c
}

// WithDefaultCacheStorage sets the cache storage type to the default value.
func (c *Config) WithDefaultCacheStorage() *Config {
	c.CacheStorage = DefaultCacheStorage
	return c
}

// WithGCPProjectID sets the GCP ProjectID to the given value.
func (c *Config) WithGCPProjectID(id string) *Config {
	c.GCPProjectID = id
	return c
}

// WithLibrarianAddrs sets the librarian addresses to the given value.
func (c *Config) WithLibrarianAddrs(librarianAddrs []*net.TCPAddr) *Config {
	if librarianAddrs == nil {
		return c.WithDefaultLibrarianAddrs()
	}
	c.LibrarianAddrs = librarianAddrs
	return c
}

// WithDefaultLibrarianAddrs sets the librarian addresses to a single address of the default IP
// and port.
func (c *Config) WithDefaultLibrarianAddrs() *Config {
	addrStr := fmt.Sprintf("%s:%d", DefaultLibrarianIP, DefaultLibrarianPort)
	addr, err := net.ResolveTCPAddr("tcp4", addrStr)
	util.MaybePanic(err) // should never happen
	c.LibrarianAddrs = []*net.TCPAddr{addr}
	return c
}
