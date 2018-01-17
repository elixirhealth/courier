package server

import (
	"fmt"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/elxirhealth/courier/pkg/base/server/test"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestBaseServer_Serve_ok(t *testing.T) {
	c := NewDefaultBaseConfig()
	c.Profile = false
	srv1 := &pingPong{NewBaseServer(c)}
	registerFunc := func(s *grpc.Server) { test.RegisterPingPongServer(s, srv1) }

	up := make(chan *pingPong, 1)
	go func() {
		err := srv1.Serve(registerFunc, func() { up <- srv1 })
		assert.Nil(t, err)
	}()

	srv1.WaitUntilStarted()
	srv2 := <-up
	assert.Equal(t, srv1, srv2)

	// set up clients
	conn, err := grpc.Dial(fmt.Sprintf(":%d", c.ServerPort), grpc.WithInsecure())
	assert.NotNil(t, conn)
	assert.Nil(t, err)

	// confirm ok health check
	clientHealth := healthpb.NewHealthClient(conn)
	ctx1, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	rp, err := clientHealth.Check(ctx1, &healthpb.HealthCheckRequest{})
	cancel()
	assert.Nil(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, rp.Status)

	srv1.StopServer()
}

func TestBaseServer_Serve_err(t *testing.T) {
	c := NewDefaultBaseConfig()
	c.ServerPort = 10000000 // bad port
	srv := &pingPong{NewBaseServer(c)}
	registerFunc := func(s *grpc.Server) { test.RegisterPingPongServer(s, srv) }

	up := make(chan *pingPong, 1)
	err := srv.Serve(registerFunc, func() { up <- srv })
	assert.NotNil(t, err)
}

func TestBaseServer_startAuxRoutines(t *testing.T) {
	c := &BaseConfig{
		ServerPort:           10100,
		MetricsPort:          10132,
		ProfilerPort:         10164,
		MaxConcurrentStreams: DefaultMaxConcurrentStreams,
		LogLevel:             zap.InfoLevel,
		Profile:              true,
	}
	b := NewBaseServer(c)
	b.startAuxRoutines()

	// confirm ok metrics
	metricsAddr := fmt.Sprintf("http://localhost:%d/metrics", c.MetricsPort)
	resp, err := http.Get(metricsAddr)
	assert.Nil(t, err)
	assert.Equal(t, "200 OK", resp.Status)

	// confirm ok debug pprof info
	profilerAddr := fmt.Sprintf("http://localhost:%d/debug/pprof", c.ProfilerPort)
	resp, err = http.Get(profilerAddr)
	assert.Nil(t, err)
	assert.Equal(t, "200 OK", resp.Status)

	// confirm Stop signal stops things
	close(b.stopped) // a bit of a hack, but required to simulate server stopping
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	<-b.Stop
}

func TestBaseServer_State(t *testing.T) {
	s := NewBaseServer(&BaseConfig{})
	assert.Equal(t, Starting, s.State())

	close(s.started)
	assert.Equal(t, Started, s.State())

	close(s.Stop)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, Stopping, s.State())

	close(s.stopped)
	assert.Equal(t, Stopped, s.State())
}

type pingPong struct {
	*BaseServer
}

func (*pingPong) Ping(context.Context, *test.PingRequest) (*test.PingResponse, error) {
	return &test.PingResponse{Pong: true}, nil
}
