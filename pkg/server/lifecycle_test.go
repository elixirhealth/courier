package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TODO (drausin) add Start test when have in-memory Cache

func TestCourier_doEvictions(t *testing.T) {
	c, err := newCourier(NewDefaultConfig())
	assert.Nil(t, err)
	c.config.Cache.EvictionPeriod = 10 * time.Millisecond
	testCache := &fixedCache{}
	c.cache = testCache

	go c.doEvictions()
	close(c.BaseServer.Stop)

	time.Sleep(2 * c.config.Cache.EvictionPeriod)
	testCache.mu.Lock()
	assert.True(t, testCache.evictCalls > 0)
	testCache.mu.Unlock()
}
