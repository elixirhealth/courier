package server

import (
	"net"
	"strings"
	"time"

	"github.com/drausin/libri/libri/common/subscribe"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/elixirhealth/service-base/pkg/server"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultLibriGetTimeout is the default timeout for libri Get requests.
	DefaultLibriGetTimeout = 3 * time.Second

	// DefaultLibriPutTimeout is the default timeout for libri Put requests.
	DefaultLibriPutTimeout = 3 * time.Second

	// DefaultLibriPutQueueSize is the default size of the libri Put queue.
	DefaultLibriPutQueueSize = 64

	// DefaultNLibriPutters is the default number of libri putters.
	DefaultNLibriPutters = 16

	// DefaultCatalogPutTimeout is the default timeout for catalog Put requests.
	DefaultCatalogPutTimeout = 1 * time.Second

	// DefaultKeyGetTimeout is the default timeout for key Get requests.
	DefaultKeyGetTimeout = 1 * time.Second

	// DefaultCatalogPutQueueSize is the default size of the catalog Put queue.
	DefaultCatalogPutQueueSize = 256

	// DefaultNCatalogPutters is the default number of catalog putters.
	DefaultNCatalogPutters = 16
)

var (
	// DefaultSubscribeToParams are the default subscription.ToParameters used by the courier.
	DefaultSubscribeToParams = &subscribe.ToParameters{
		NSubscriptions:  4,
		FPRate:          1.0,
		Timeout:         subscribe.DefaultTimeout,
		MaxErrRate:      subscribe.DefaultMaxErrRate,
		RecentCacheSize: subscribe.DefaultRecentCacheSize,
	}
)

// Config is the config for a Courier instance.
type Config struct {
	*server.BaseConfig
	ClientIDFilepath string

	Librarians        []*net.TCPAddr
	LibriGetTimeout   time.Duration
	LibriPutTimeout   time.Duration
	LibriPutQueueSize uint
	NLibriPutters     uint

	Catalog             *net.TCPAddr
	CatalogPutTimeout   time.Duration
	SubscribeTo         *subscribe.ToParameters
	CatalogPutQueueSize uint
	NCatalogPutters     uint

	Key           *net.TCPAddr
	KeyGetTimeout time.Duration

	GCPProjectID string
	DBUrl        string
	Storage      *storage.Parameters
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
		WithDefaultCatalogPutTimeout().
		WithDefaultSubscribeTo().
		WithDefaultCatalogPutQueueSize().
		WithDefaultNCatalogPutters().
		WithDefaultKeyGetTimeout().
		WithDefaultStorage()
}

// MarshalLogObject writes the config to the given object encoder.
func (c *Config) MarshalLogObject(oe zapcore.ObjectEncoder) error {
	if err := c.BaseConfig.MarshalLogObject(oe); err != nil {
		return err
	}
	if c.ClientIDFilepath != "" {
		oe.AddString(logClientIDFilepath, c.ClientIDFilepath)
	}
	las := make([]string, len(c.Librarians))
	for i, la := range c.Librarians {
		las[i] = la.String()
	}
	oe.AddString(logLibrarians, strings.Join(las, " "))
	oe.AddDuration(logLibriGetTimeout, c.LibriGetTimeout)
	oe.AddDuration(logLibriPutTimeout, c.LibriPutTimeout)
	oe.AddUint(logLibriPutQueueSize, c.LibriPutQueueSize)
	oe.AddUint(logNLibriPutters, c.NLibriPutters)
	oe.AddString(logCatalog, c.Catalog.String())
	oe.AddDuration(logCatalogPutTimeout, c.CatalogPutTimeout)
	// TODO (drausin) once Libri supports
	/*
		if err := oe.AddObject(logSubscribeTo, c.SubscribeTo); err != nil {
			return err
		}
	*/
	oe.AddUint(logCatalogPutQueueSize, c.CatalogPutQueueSize)
	oe.AddUint(logNCatalogPutters, c.NCatalogPutters)

	if c.GCPProjectID != "" {
		oe.AddString(logGCPProjectID, c.GCPProjectID)
	}
	if err := oe.AddObject(logStorage, c.Storage); err != nil {
		return err
	}
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

// WithCatalogAddr sets the catalog addresses to the given value.
func (c *Config) WithCatalogAddr(addr *net.TCPAddr) *Config {
	c.Catalog = addr
	return c
}

// WithCatalogPutTimeout sets the catalog Put request timeout to the given value or to the default
// if it is zero-valued.
func (c *Config) WithCatalogPutTimeout(t time.Duration) *Config {
	if t == 0 {
		return c.WithDefaultCatalogPutTimeout()
	}
	c.CatalogPutTimeout = t
	return c
}

// WithDefaultCatalogPutTimeout sets the catalog Put request timeout to the default value.
func (c *Config) WithDefaultCatalogPutTimeout() *Config {
	c.CatalogPutTimeout = DefaultCatalogPutTimeout
	return c
}

// WithCatalogPutQueueSize sets the catalog Put queue size to the given value or to the default if
// it is zero-valued.
func (c *Config) WithCatalogPutQueueSize(s uint) *Config {
	if s == 0 {
		return c.WithDefaultCatalogPutQueueSize()
	}
	c.CatalogPutQueueSize = s
	return c
}

// WithDefaultCatalogPutQueueSize sets the catalog Put queue size to the default value.
func (c *Config) WithDefaultCatalogPutQueueSize() *Config {
	c.CatalogPutQueueSize = DefaultCatalogPutQueueSize
	return c
}

// WithSubscribeTo sets the SubscribeTo parameters to the given value or the defaults if it is nil.
func (c *Config) WithSubscribeTo(p *subscribe.ToParameters) *Config {
	if p == nil {
		return c.WithDefaultSubscribeTo()
	}
	c.SubscribeTo = p
	return c
}

// WithDefaultSubscribeTo set the SubscribeTo parameters to their default values.
func (c *Config) WithDefaultSubscribeTo() *Config {
	c.SubscribeTo = DefaultSubscribeToParams
	return c
}

// WithNCatalogPutters sets the number of catalog putters to the given value or the default if it is
// zero.
func (c *Config) WithNCatalogPutters(n uint) *Config {
	if n == 0 {
		return c.WithDefaultNCatalogPutters()
	}
	c.NCatalogPutters = n
	return c
}

// WithDefaultNCatalogPutters sets the number of catalog putters to the default value.
func (c *Config) WithDefaultNCatalogPutters() *Config {
	c.NCatalogPutters = DefaultNCatalogPutters
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
func (c *Config) WithCache(p *storage.Parameters) *Config {
	if p == nil {
		return c.WithDefaultStorage()
	}
	c.Storage = p
	return c
}

// WithDefaultStorage set the Storage parameters to their default values.
func (c *Config) WithDefaultStorage() *Config {
	c.Storage = storage.NewDefaultParameters()
	return c
}

// WithLibrarianAddrs sets the librarian addresses to the given value.
func (c *Config) WithLibrarianAddrs(librarianAddrs []*net.TCPAddr) *Config {
	c.Librarians = librarianAddrs
	return c
}

// WithKeyAddr sets the key addresses to the given value.
func (c *Config) WithKeyAddr(addr *net.TCPAddr) *Config {
	c.Key = addr
	return c
}

// WithKeyGetTimeout sets the key Get request timeout to the given value or to the default
// if it is zero-valued.
func (c *Config) WithKeyGetTimeout(t time.Duration) *Config {
	if t == 0 {
		return c.WithDefaultKeyGetTimeout()
	}
	c.KeyGetTimeout = t
	return c
}

// WithDefaultKeyGetTimeout sets the key Get request timeout to the default value.
func (c *Config) WithDefaultKeyGetTimeout() *Config {
	c.KeyGetTimeout = DefaultKeyGetTimeout
	return c
}

// WithDBUrl sets the DB URL to the given value.
func (c *Config) WithDBUrl(dbURL string) *Config {
	c.DBUrl = dbURL
	return c
}
