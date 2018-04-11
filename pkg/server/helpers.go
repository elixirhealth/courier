package server

import (
	"context"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/drausin/libri/libri/common/ecid"
	libriapi "github.com/drausin/libri/libri/librarian/api"
	"github.com/elixirhealth/catalog/pkg/catalogapi"
	"github.com/elixirhealth/courier/pkg/server/storage"
	"github.com/elixirhealth/courier/pkg/server/storage/datastore"
	"github.com/elixirhealth/courier/pkg/server/storage/memory"
	"github.com/elixirhealth/courier/pkg/server/storage/postgres"
	"github.com/elixirhealth/key/pkg/keyapi"
	bstorage "github.com/elixirhealth/service-base/pkg/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrInvalidCacheStorageType indicates when a storage type is not expected.
	ErrInvalidCacheStorageType = errors.New("invalid cache storage type")
)

func getClientID(config *Config) (ecid.ID, error) {
	if config.ClientIDFilepath != "" {
		return ecid.FromPrivateKeyFile(config.ClientIDFilepath)
	}
	return ecid.NewRandom(), nil
}

func getCache(config *Config, logger *zap.Logger) (storage.Cache, storage.AccessRecorder, error) {
	switch config.Storage.Type {
	case bstorage.Postgres:
		return postgres.New(config.DBUrl, config.Storage, logger)
	case bstorage.DataStore:
		return datastore.New(config.GCPProjectID, config.Storage, logger)
	case bstorage.Memory:
		c, ar := memory.New(config.Storage, logger)
		return c, ar, nil
	default:
		return nil, nil, ErrInvalidCacheStorageType
	}
}

type catalogPutter interface {
	maybePut(key []byte, value *libriapi.Document) error
	put(pr *catalogapi.PublicationReceipt) error
}

type catalogPutterImpl struct {
	config  *Config
	logger  *zap.Logger
	catalog catalogapi.CatalogClient
	key     keyapi.KeyClient
}

func (p *catalogPutterImpl) maybePut(key []byte, value *libriapi.Document) error {
	ct, ok := value.Contents.(*libriapi.Document_Envelope)
	if !ok {
		// do nothing for non-envelope docs
		return nil
	}

	// create publication receipt for catalog from envelope
	pr := &catalogapi.PublicationReceipt{
		EnvelopeKey:     key,
		EntryKey:        ct.Envelope.EntryKey,
		AuthorPublicKey: ct.Envelope.AuthorPublicKey,
		ReaderPublicKey: ct.Envelope.ReaderPublicKey,
	}
	return p.put(pr)
}

func (p *catalogPutterImpl) put(pr *catalogapi.PublicationReceipt) error {
	pr.ReceivedTime = time.Now().UnixNano() / 1E3
	if pr.AuthorEntityId == "" || pr.ReaderEntityId == "" {
		aEntityID, rEntityID, err := p.getEntityIDs(pr.AuthorPublicKey, pr.ReaderPublicKey)
		if err != nil {
			return err
		}
		pr.AuthorEntityId = aEntityID
		pr.ReaderEntityId = rEntityID
	}
	rq := &catalogapi.PutRequest{Value: pr}
	p.logger.Debug("putting publication", logPutPublication(rq)...)
	bo := newTimeoutExpBackoff(p.config.CatalogPutTimeout)
	ctx, cancel := context.WithTimeout(context.Background(),
		p.config.CatalogPutTimeout)
	defer cancel()
	op := func() error {
		_, err := p.catalog.Put(ctx, rq)
		return err
	}
	if err := backoff.Retry(op, bo); err != nil {
		return err
	}
	p.logger.Debug("put publication", logPutPublication(rq)...)
	return nil
}

func (p *catalogPutterImpl) getEntityIDs(authorPub, readerPub []byte) (string, string, error) {
	rq := &keyapi.GetPublicKeyDetailsRequest{
		PublicKeys: [][]byte{authorPub, readerPub},
	}
	p.logger.Debug("getting entity IDs", logGetEntityIDs(rq)...)
	bo := newTimeoutExpBackoff(p.config.KeyGetTimeout)
	ctx, cancel := context.WithTimeout(context.Background(),
		p.config.KeyGetTimeout)
	defer cancel()
	var rp *keyapi.GetPublicKeyDetailsResponse
	op := func() error {
		var err error
		rp, err = p.key.GetPublicKeyDetails(ctx, rq)
		if err == nil || status.Convert(err).Code() == codes.NotFound {
			return nil
		}
		p.logger.Debug("error getting public key details", zap.Error(err))
		return err
	}
	if err := backoff.Retry(op, bo); err != nil {
		return "", "", err
	}
	if rp == nil {
		p.logger.Debug("entity IDs not found", logGetEntityIDs(rq)...)
		return "", "", nil
	}
	authorEntityID := rp.PublicKeyDetails[0].EntityId
	readerEntityID := rp.PublicKeyDetails[1].EntityId
	p.logger.Debug("got entity IDs", logGotEntityIDs(rq, rp)...)
	return authorEntityID, readerEntityID, nil
}
