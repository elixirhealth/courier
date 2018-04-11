package server

import (
	"math/rand"
	"testing"

	"github.com/elixirhealth/catalog/pkg/catalogapi"
	"github.com/elixirhealth/key/pkg/keyapi"
	"github.com/elixirhealth/service-base/pkg/util"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCatalogPutterImpl_put_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	authorEntityID, readerEntityID := "author entity ID", "reader entity ID"

	cc := &fixedCatalogClient{}
	cp := &catalogPutterImpl{
		config:  okConfig,
		logger:  zap.NewNop(), // logging.NewDevLogger(zap.DebugLevel),
		catalog: cc,
		key: &fixedKeyClient{
			getPKD: []*keyapi.PublicKeyDetail{
				{EntityId: authorEntityID},
				{EntityId: readerEntityID},
			},
		},
	}

	// entity IDs already exist in PR
	pr := &catalogapi.PublicationReceipt{
		EnvelopeKey:     util.RandBytes(rng, 32),
		EntryKey:        util.RandBytes(rng, 32),
		AuthorPublicKey: util.RandBytes(rng, 33),
		AuthorEntityId:  "some other author ID",
		ReaderPublicKey: util.RandBytes(rng, 33),
		ReaderEntityId:  "some other reader ID",
	}
	err := cp.put(pr)
	assert.Nil(t, err)
	assert.Equal(t, pr.AuthorEntityId, cc.putRq.Value.AuthorEntityId)
	assert.Equal(t, pr.ReaderEntityId, cc.putRq.Value.ReaderEntityId)

	// entity IDs don't exist
	pr = &catalogapi.PublicationReceipt{
		EnvelopeKey:     util.RandBytes(rng, 32),
		EntryKey:        util.RandBytes(rng, 32),
		AuthorPublicKey: util.RandBytes(rng, 33),
		ReaderPublicKey: util.RandBytes(rng, 33),
	}
	err = cp.put(pr)
	assert.Nil(t, err)
	assert.Equal(t, authorEntityID, cc.putRq.Value.AuthorEntityId)
	assert.Equal(t, readerEntityID, cc.putRq.Value.ReaderEntityId)
}

func TestCatalogPutterImpl_getEntityIDs_ok(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	authorPub, readerPub := util.RandBytes(rng, 33), util.RandBytes(rng, 33)
	authorEntityID, readerEntityID := "author entity ID", "reader entity ID"

	cp := &catalogPutterImpl{
		config: okConfig,
		logger: zap.NewNop(), // logging.NewDevLogger(zap.DebugLevel),
		key: &fixedKeyClient{
			getPKD: []*keyapi.PublicKeyDetail{
				{EntityId: authorEntityID},
				{EntityId: readerEntityID},
			},
		},
	}

	gotAuthorID, gotReaderID, err := cp.getEntityIDs(authorPub, readerPub)
	assert.Nil(t, err)
	assert.Equal(t, authorEntityID, gotAuthorID)
	assert.Equal(t, readerEntityID, gotReaderID)
}

func TestCourier_getEntityIDs_err(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	authorPub, readerPub := util.RandBytes(rng, 33), util.RandBytes(rng, 33)

	cp := &catalogPutterImpl{
		config: okConfig,
		logger: zap.NewNop(), // logging.NewDevLogger(zap.DebugLevel),
	}

	// not found
	rpErr := status.Error(codes.NotFound, keyapi.ErrNoSuchPublicKey.Error())
	cp.key = &fixedKeyClient{getPKDErr: rpErr}
	gotAuthorID, gotReaderID, err := cp.getEntityIDs(authorPub, readerPub)
	assert.Nil(t, err)
	assert.Empty(t, gotAuthorID)
	assert.Empty(t, gotReaderID)

	// other error
	cp.key = &fixedKeyClient{getPKDErr: errTest}
	gotAuthorID, gotReaderID, err = cp.getEntityIDs(authorPub, readerPub)
	assert.Equal(t, errTest, err)
	assert.Empty(t, gotAuthorID)
	assert.Empty(t, gotReaderID)
}
