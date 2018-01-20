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
	// ServerPort is the port from which to serve requests for the main service.
	ServerPort uint

	// MetricsPort is the port from which to serve Prometheus metrics.
	MetricsPort uint

	// ProfilerPort is the port from which to serve profiler endpoints.
	ProfilerPort uint

	// MaxConcurrentStreams is the maximum number of concurrent streams for each server
	// transport.
	MaxConcurrentStreams uint32

	// LogLevel is the log level for the service logger.
	LogLevel zapcore.Level

	// Profile indicates whether the profiler endpoints are enabled.
	Profile bool
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

// WithServerPort sets the main server port to the given value or the default if it is zero.
func (c *BaseConfig) WithServerPort(p uint) *BaseConfig {
	if p == 0 {
		return c.WithDefaultServerPort()
	}
	c.ServerPort = p
	return c
}

// WithDefaultServerPort sets the main server port to the default value.
func (c *BaseConfig) WithDefaultServerPort() *BaseConfig {
	c.ServerPort = DefaultServerPort
	return c
}

// WithMetricsPort sets the metrics port to the given value or the default if it is zero.
func (c *BaseConfig) WithMetricsPort(p uint) *BaseConfig {
	if p == 0 {
		return c.WithDefaultServerPort()
	}
	c.MetricsPort = p
	return c
}

// WithDefaultMetricsPort sets the metrics port to the default value.
func (c *BaseConfig) WithDefaultMetricsPort() *BaseConfig {
	c.ServerPort = DefaultMetricsPort
	return c
}

// WithProfilerPort sets the profiler port to the given value or the default if is zero.
func (c *BaseConfig) WithProfilerPort(p uint) *BaseConfig {
	if p == 0 {
		return c.WithDefaultServerPort()
	}
	c.ProfilerPort = p
	return c
}

// WithDefaultProfilerPort sets the profiler port to the default value.
func (c *BaseConfig) WithDefaultProfilerPort() *BaseConfig {
	c.ServerPort = DefaultServerPort
	return c
}

// WithMaxConcurrentStreams set the max concurrent streams for a server transport to the given
// value or the default if it is zero.
func (c *BaseConfig) WithMaxConcurrentStreams(m uint32) *BaseConfig {
	if m == 0 {
		return c.WithDefaultMaxConcurrentStreams()
	}
	c.MaxConcurrentStreams = m
	return c
}

// WithDefaultMaxConcurrentStreams sets the max concurrent streams to the default value.
func (c *BaseConfig) WithDefaultMaxConcurrentStreams() *BaseConfig {
	c.MaxConcurrentStreams = DefaultMaxConcurrentStreams
	return c
}

// WithLogLevel sets the log level to the given value.
func (c *BaseConfig) WithLogLevel(l zapcore.Level) *BaseConfig {
	c.LogLevel = l
	return c
}

// WithDefaultLogLevel sets the log level to the default value.
func (c *BaseConfig) WithDefaultLogLevel() *BaseConfig {
	c.LogLevel = DefaultLogLevel
	return c
}

// WithProfile sets whether to enable the profiler endpoints.
func (c *BaseConfig) WithProfile(on bool) *BaseConfig {
	c.Profile = on
	return c
}

// WithDefaultProfile sets the default value for whether to enable the profiler.
func (c *BaseConfig) WithDefaultProfile() *BaseConfig {
	c.Profile = DefaultProfile
	return c
}
