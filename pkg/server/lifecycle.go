package server

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	cerrors "github.com/drausin/libri/libri/common/errors"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elixirhealth/catalog/pkg/catalogapi"
	api "github.com/elixirhealth/courier/pkg/courierapi"
	"github.com/elixirhealth/courier/pkg/server/storage/postgres/migrations"
	"github.com/elixirhealth/service-base/pkg/server"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/golang/protobuf/proto"
	bindata "github.com/mattes/migrate/source/go-bindata"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	evictorErrQueueSize       = 4
	evictorMaxErrRate         = 0.75
	libriPutterErrQueueSize   = 24
	libriPutterMaxErrRate     = 1.0 / 3 // 3 different errors statements return an error
	catalogPutterErrQueueSize = 4
	catalogPutterMaxErrRate   = 0.75
)

// Start starts the server and eviction routines.
func Start(config *Config, up chan *Courier) error {
	c, err := newCourier(config)
	if err != nil {
		return err
	}

	if err = c.maybeMigrateDB(); err != nil {
		return err
	}

	// start Courier aux routines
	go c.startEvictor()
	go c.startLibriPutters()
	go c.startCatalogPutters()
	go c.startPublicationReceiver()

	registerServer := func(s *grpc.Server) { api.RegisterCourierServer(s, c) }
	return c.Serve(registerServer, func() { up <- c })
}

// StopServer stops auxiliary routines and the server.
func (c *Courier) StopServer() {
	c.subscribeTo.End()
	c.BaseServer.StopServer()
	err := c.cache.Close()
	cerrors.MaybePanic(err)
}

func (c *Courier) maybeMigrateDB() error {
	if c.config.Storage.Type != bstorage.Postgres {
		return nil
	}

	m := bstorage.NewBindataMigrator(
		c.config.DBUrl,
		bindata.Resource(migrations.AssetNames(), migrations.Asset),
		&bstorage.ZapLogger{Logger: c.Logger},
	)
	return m.Up()
}

func (c *Courier) startEvictor() {
	// monitor non-fatal errors, sending fatal err if too many
	errs := make(chan error, 2) // non-fatal errs and nils
	fatal := make(chan error)   // signals fatal end
	go cerrors.MonitorRunningErrors(errs, fatal, evictorErrQueueSize, evictorMaxErrRate,
		c.Logger)
	go func() {
		err := <-fatal
		c.Logger.Error("fatal eviction error", zap.Error(err))
		c.StopServer()
	}()

	maxInitialWait := int64(c.config.Storage.EvictionPeriod)
	initialWait, err := rand.Int(rand.Reader, big.NewInt(maxInitialWait))
	errs <- err
	time.Sleep(time.Duration(initialWait.Int64()))
	for {
		pause := make(chan struct{})
		go func() {
			time.Sleep(c.config.Storage.EvictionPeriod)
			maybeClose(pause)
		}()
		go func() {
			<-c.BaseServer.Stop
			maybeClose(pause)
		}()
		<-pause

		if c.BaseServer.State() >= server.Stopping {
			return
		}
		err = c.cache.EvictNext()
		errs <- err
		if err != nil {
			c.Logger.Error("error evicting next batch", zap.Error(err))
		}
	}
}

func (c *Courier) startLibriPutters() {
	// monitor non-fatal errors, sending fatal err if too many
	chMu := new(sync.Mutex)
	errs := make(chan error, 2*c.config.NLibriPutters)  // non-fatal errs and nils
	fatal := make(chan error, 2*c.config.NLibriPutters) // signals fatal end
	go cerrors.MonitorRunningErrors(errs, fatal, libriPutterErrQueueSize, libriPutterMaxErrRate,
		c.Logger)
	go func() {
		err := <-fatal
		c.Logger.Error("fatal libri libriPutter error", zap.Error(err))
		c.StopServer()
	}()
	go func() {
		<-c.BaseServer.Stop
		chMu.Lock()
		close(c.libriPutQueue)
		chMu.Unlock()
	}()

	wg1 := new(sync.WaitGroup)
	for i := uint(0); i < c.config.NLibriPutters; i++ {
		wg1.Add(1)
		go func(wg2 *sync.WaitGroup, j uint) {
			defer wg2.Done()
			var msg string
			for key := range c.libriPutQueue {
				if c.BaseServer.State() >= server.Stopping {
					return
				}
				c.Logger.Debug("publishing document to libri",
					zap.String(logKey, hex.EncodeToString(key)))
				docBytes, err := c.cache.Get(key)

				msg = "error getting document from cache"
				if ok := c.handleRunningErr(err, errs, msg, key); !ok {
					continue
				}
				doc := &libriapi.Document{}
				if err = proto.Unmarshal(docBytes, doc); err != nil {
					fatal <- err
					return
				}

				docKey, err := c.libriPublisher.Publish(doc,
					libriapi.GetAuthorPub(doc), c.libriPutter)
				msg = "error publishing document to libri"
				if ok := c.handleRunningErr(err, errs, msg, key); !ok {
					// add back onto queue so we don't drop it
					chMu.Lock()
					if c.BaseServer.State() < server.Stopping {
						c.libriPutQueue <- key
					}
					chMu.Unlock()
					continue
				}

				err = c.accessRecorder.LibriPut(key)
				msg = "error updating document access record"
				if ok := c.handleRunningErr(err, errs, msg, key); !ok {
					continue
				}

				c.Logger.Info("published document to libri",
					zap.Stringer(logKey, docKey),
				)
			}
		}(wg1, i)
	}
	wg1.Wait()
}

func (c *Courier) startPublicationReceiver() {
	if err := c.subscribeTo.Begin(); err != nil {
		c.Logger.Error("fatal subscribeTo error", zap.Error(err))
		c.StopServer()
	}
}

func (c *Courier) startCatalogPutters() {
	// monitor non-fatal errors, sending fatal err if too many
	errs := make(chan error, 2*c.config.NCatalogPutters)  // non-fatal errs and nils
	fatal := make(chan error, 2*c.config.NCatalogPutters) // signals fatal end
	mu := new(sync.Mutex)
	go cerrors.MonitorRunningErrors(errs, fatal, catalogPutterErrQueueSize,
		catalogPutterMaxErrRate, c.Logger)
	go func() {
		err := <-fatal
		c.Logger.Error("fatal subscribeTo error", zap.Error(err))
		c.StopServer()
	}()
	go func() {
		<-c.BaseServer.Stop
		select {
		case <-c.catalogPutQueue: // already closed
		default:
			mu.Lock()
			close(c.catalogPutQueue)
			mu.Unlock()
		}
	}()

	wg1 := new(sync.WaitGroup)
	for i := uint(0); i < c.config.NLibriPutters; i++ {
		wg1.Add(1)
		go func(wg2 *sync.WaitGroup, j uint) {
			defer wg2.Done()
			for kp := range c.catalogPutQueue {
				pub := kp.Value
				if c.BaseServer.State() >= server.Stopping {
					return
				}
				pr := &catalogapi.PublicationReceipt{
					EnvelopeKey:     pub.EnvelopeKey,
					EntryKey:        pub.EntryKey,
					AuthorPublicKey: pub.AuthorPublicKey,
					ReaderPublicKey: pub.ReaderPublicKey,
				}
				err := c.catalogPutter.put(pr)
				if err != nil {
					// add back onto queue so we don't drop it
					mu.Lock()
					if c.BaseServer.State() < server.Stopping {
						c.catalogPutQueue <- kp
					}
					mu.Unlock()
				}
				errs <- err
			}
		}(wg1, i)
	}
	wg1.Wait()
}

func newTimeoutExpBackoff(timeout time.Duration) backoff.BackOff {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = timeout
	return bo
}

func (c *Courier) handleRunningErr(err error, errs chan error, logMsg string, key []byte) bool {
	select {
	case errs <- err:
	default:
	}
	if err != nil {
		c.Logger.Error(logMsg,
			zap.String(logKey, hex.EncodeToString(key)),
			zap.Error(err),
		)
		return false
	}
	return true
}

func maybeClose(ch chan struct{}) {
	select {
	case <-ch: // already closed
	default:
		close(ch)
	}
}
