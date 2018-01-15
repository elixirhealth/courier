package server

import (
	"time"

	"github.com/elxirhealth/courier/pkg/base/server"
	api "github.com/elxirhealth/courier/pkg/courierapi"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Start starts the server and eviction routines.
func Start(config *Config) error {
	c, err := newCourier(config)
	if err != nil {
		return err
	}

	// start courier aux routines
	go c.doEvictions()

	registerServer := func(s *grpc.Server) { api.RegisterCourierServer(s, c) }
	return c.Serve(registerServer)
}

func (c *courier) doEvictions() {
	for {
		pause := make(chan struct{})
		go func() {
			time.Sleep(c.config.Cache.EvictionPeriod)
			maybeClose(pause)
		}()
		go func() {
			<-c.BaseServer.Stop
			maybeClose(pause)
		}()
		<-pause

		if c.BaseServer.State() >= server.Stopped {
			return
		}
		if err := c.cache.EvictNext(); err != nil {
			c.Logger.Error("fatal eviction error", zap.Error(err))
			c.StopServer()
			return
		}
	}
}

func maybeClose(ch chan struct{}) {
	select {
	case <-ch: // already closed
	default:
		close(ch)
	}
}
