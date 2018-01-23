package server

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/drausin/libri/libri/common/errors"
	"github.com/elxirhealth/courier/pkg/cache"
	"github.com/elxirhealth/service-base/pkg/server"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultLibriGetTimeout is the default timeout for libri Get requests.
	DefaultLibriGetTimeout = 5 * time.Second

	// DefaultLibriPutTimeout is the default timeout for libri Put requests.
	DefaultLibriPutTimeout = 5 * time.Second

	// DefaultLibriPutQueueSize is the default size of the libri Put queue.
	DefaultLibriPutQueueSize = 64

	// DefaultNLibriPutters is the default number of libri putters.
	DefaultNLibriPutters = 16

	// DefaultLibrarianPort is the default port of the librarian server.
	DefaultLibrarianPort = 20100

	// DefaultLibrarianHost is the default IP of of the librarian server.
	DefaultLibrarianHost = "localhost"
)

// Config is the config for a Courier instance.
type Config struct {
	*server.BaseConfig
	LibriGetTimeout   time.Duration
	LibriPutTimeout   time.Duration
	LibriPutQueueSize uint
	NLibriPutters     uint
	ClientIDFilepath  string
	GCPProjectID      string
	Cache             *cache.Parameters
	Librarians        []*net.TCPAddr
}

// NewDefaultConfig create a new config instance with default values.
func NewDefaultConfig() *Config {
	config := &Config{
		BaseConfig: server.NewDefaultBaseConfig(),
	}
	return config.
		WithDefaultLibriGetTimeout().
		WithDefaultLibriPutTimeout().
		WithDefaultLibriPutQueueSize().
		WithDefaultNLibriPutters().
		WithDefaultCache().
		WithDefaultLibrarianAddrs()
}

// MarshalLogObject writes the config to the given object encoder.
func (c *Config) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	if err := c.Cache.MarshalLogObject(oe); err != nil {
		return err
	}
	oe.AddDuration(logLibriGetTimeout, c.LibriGetTimeout)
	oe.AddDuration(logLibriPutTimeout, c.LibriPutTimeout)
	oe.AddUint(logLibriPutQueueSize, c.LibriPutQueueSize)
	oe.AddUint(logNLibriPutters, c.NLibriPutters)
	if c.ClientIDFilepath != "" {
		oe.AddString(logClientIDFilepath, c.ClientIDFilepath)
	}
	if c.GCPProjectID != "" {
		oe.AddString(logGCPProjectID, c.GCPProjectID)
	}
	if err := oe.AddObject(logCache, c.Cache); err != nil {
		return err
	}
	las := make([]string, len(c.Librarians))
	for i, la := range c.Librarians {
		las[i] = la.String()
	}
	oe.AddString(logLibrarians, strings.Join(las, " "))
	return nil
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
	c.LibriPutTimeout = t
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

// WithNLibriPutters sets the number of libri putters to the given value or the default if it is
// zero.
func (c *Config) WithNLibriPutters(n uint) *Config {
	if n == 0 {
		return c.WithDefaultNLibriPutters()
	}
	c.NLibriPutters = n
	return c
}

// WithDefaultNLibriPutters sets the number of libri putters to the default value.
func (c *Config) WithDefaultNLibriPutters() *Config {
	c.NLibriPutters = DefaultNLibriPutters
	return c
}

// WithClientIDFilepath sets the file path for the local *.der file containing the clientID private
// key.
func (c *Config) WithClientIDFilepath(fp string) *Config {
	c.ClientIDFilepath = fp
	return c
}

// WithGCPProjectID sets the GCP ProjectID to the given value.
func (c *Config) WithGCPProjectID(id string) *Config {
	c.GCPProjectID = id
	return c
}

// WithCache sets the cache parameters to the given value or the defaults if it is nil.
func (c *Config) WithCache(p *cache.Parameters) *Config {
	if p == nil {
		return c.WithDefaultCache()
	}
	c.Cache = p
	return c
}

// WithDefaultCache set the Cache parameters to their default values.
func (c *Config) WithDefaultCache() *Config {
	c.Cache = cache.NewDefaultParameters()
	return c
}

// WithLibrarianAddrs sets the librarian addresses to the given value.
func (c *Config) WithLibrarianAddrs(librarianAddrs []*net.TCPAddr) *Config {
	if librarianAddrs == nil {
		return c.WithDefaultLibrarianAddrs()
	}
	c.Librarians = librarianAddrs
	return c
}

// WithDefaultLibrarianAddrs sets the librarian addresses to a single address of the default IP
// and port.
func (c *Config) WithDefaultLibrarianAddrs() *Config {
	addrStr := fmt.Sprintf("%s:%d", DefaultLibrarianHost, DefaultLibrarianPort)
	addr, err := net.ResolveTCPAddr("tcp4", addrStr)
	errors.MaybePanic(err) // should never happen
	c.Librarians = []*net.TCPAddr{addr}
	return c
}
