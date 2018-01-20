package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestNewDefaultConfig(t *testing.T) {
	c := NewDefaultBaseConfig()
	assert.NotEmpty(t, c.ServerPort)
	assert.NotEmpty(t, c.MetricsPort)
	assert.NotEmpty(t, c.ProfilerPort)
	assert.NotEmpty(t, c.MaxConcurrentStreams)
}

func TestConfig_WithServerPort(t *testing.T) {
	c1, c2, c3 := &BaseConfig{}, &BaseConfig{}, &BaseConfig{}
	c1.WithDefaultServerPort()
	assert.Equal(t, c1.ServerPort, c2.WithServerPort(0).ServerPort)
	assert.NotEqual(t, c1.ServerPort, c3.WithServerPort(1000).ServerPort)
}

func TestConfig_WithMetricsPort(t *testing.T) {
	c1, c2, c3 := &BaseConfig{}, &BaseConfig{}, &BaseConfig{}
	c1.WithDefaultMetricsPort()
	assert.Equal(t, c1.MetricsPort, c2.WithMetricsPort(0).MetricsPort)
	assert.NotEqual(t, c1.MetricsPort, c3.WithMetricsPort(1000).MetricsPort)
}

func TestConfig_WithProfilerPort(t *testing.T) {
	c1, c2, c3 := &BaseConfig{}, &BaseConfig{}, &BaseConfig{}
	c1.WithDefaultProfilerPort()
	assert.Equal(t, c1.ProfilerPort, c2.WithProfilerPort(0).ProfilerPort)
	assert.NotEqual(t, c1.ProfilerPort, c3.WithProfilerPort(1000).ProfilerPort)
}

func TestConfig_WithMaxConcurrentStreams(t *testing.T) {
	c1, c2, c3 := &BaseConfig{}, &BaseConfig{}, &BaseConfig{}
	c1.WithDefaultMaxConcurrentStreams()
	assert.Equal(t, c1.MaxConcurrentStreams, c2.WithMaxConcurrentStreams(0).MaxConcurrentStreams)
	assert.NotEqual(t, c1.MaxConcurrentStreams,
		c3.WithMaxConcurrentStreams(1000).MaxConcurrentStreams)
}

func TestConfig_WithLogLevel(t *testing.T) {
	c1, c2, c3 := &BaseConfig{}, &BaseConfig{}, &BaseConfig{}
	c1.WithDefaultLogLevel()
	assert.Equal(t, c1.LogLevel, c2.WithLogLevel(zapcore.InfoLevel).LogLevel)
	assert.NotEqual(t, c1.LogLevel, c3.WithLogLevel(zapcore.ErrorLevel).LogLevel)
}

func TestConfig_WithProfile(t *testing.T) {
	c1, c2, c3 := &BaseConfig{}, &BaseConfig{}, &BaseConfig{}
	c1.WithDefaultProfile()
	assert.Equal(t, c1.Profile, c2.WithProfile(false).Profile)
	assert.NotEqual(t, c1.Profile, c3.WithProfile(true).Profile)
}
