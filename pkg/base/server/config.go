package server

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// DefaultMaxConcurrentStreams defines the maximum number of concurrent streams for each
	// server transport.
	DefaultMaxConcurrentStreams = uint32(128)

	// DefaultServerPort is the default port on which the main server listens.
	DefaultServerPort = 10100

	// DefaultMetricsPort is the default port for Prometheus metrics.
	DefaultMetricsPort = 10101

	// DefaultProfilerPort is the default port for profiler requests.
	DefaultProfilerPort = 10102

	// DefaultLogLevel is the default log level to use.
	DefaultLogLevel = zap.InfoLevel

	// DefaultProfile is the default setting for whether the profiler is enabled.
	DefaultProfile = false

	postListenNotifyWait = 100 * time.Millisecond
)

// BaseConfig contains params needed for the base server.
type BaseConfig struct {
	ServerPort           uint
	MetricsPort          uint
	ProfilerPort         uint
	MaxConcurrentStreams uint32
	LogLevel             zapcore.Level
	Profile              bool
}

// NewDefaultBaseConfig creates a new default BaseConfig.
func NewDefaultBaseConfig() *BaseConfig {
	return &BaseConfig{
		ServerPort:           DefaultServerPort,
		MetricsPort:          DefaultMetricsPort,
		ProfilerPort:         DefaultProfilerPort,
		MaxConcurrentStreams: DefaultMaxConcurrentStreams,
		LogLevel:             DefaultLogLevel,
		Profile:              DefaultProfile,
	}
}

func (c *BaseConfig) WithServerPort(p uint) *BaseConfig {
	if p == 0 {
		return c.WithDefaultServerPort()
	}
	c.ServerPort = p
	return c
}

func (c *BaseConfig) WithDefaultServerPort() *BaseConfig {
	c.ServerPort = DefaultServerPort
	return c
}

func (c *BaseConfig) WithMetricsPort(p uint) *BaseConfig {
	if p == 0 {
		return c.WithDefaultServerPort()
	}
	c.MetricsPort = p
	return c
}

func (c *BaseConfig) WithDefaultMetricsPort() *BaseConfig {
	c.ServerPort = DefaultMetricsPort
	return c
}

func (c *BaseConfig) WithProfilerPort(p uint) *BaseConfig {
	if p == 0 {
		return c.WithDefaultServerPort()
	}
	c.ProfilerPort = p
	return c
}

func (c *BaseConfig) WithDefaultProfilerPort() *BaseConfig {
	c.ServerPort = DefaultServerPort
	return c
}

func (c *BaseConfig) WithMaxConcurrentStreams(m uint32) *BaseConfig {
	if m == 0 {
		return c.WithDefaultMaxConcurrentStreams()
	}
	c.MaxConcurrentStreams = m
	return c
}

func (c *BaseConfig) WithDefaultMaxConcurrentStreams() *BaseConfig {
	c.MaxConcurrentStreams = DefaultMaxConcurrentStreams
	return c
}

func (c *BaseConfig) WithLogLevel(l zapcore.Level) *BaseConfig {
	c.LogLevel = l
	return c
}

func (c *BaseConfig) WithDefaultLogLevel() *BaseConfig {
	c.LogLevel = DefaultLogLevel
	return c
}

func (c *BaseConfig) WithProfile(on bool) *BaseConfig {
	c.Profile = on
	return c
}
