package server

import (
	"encoding/hex"

	"github.com/elixirhealth/catalog/pkg/catalogapi"
	"github.com/elixirhealth/key/pkg/keyapi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logKey                 = "key"
	logLibriGetTimeout     = "libri_get_timeout"
	logLibriPutTimeout     = "libri_put_timeout"
	logLibriPutQueueSize   = "libri_put_queue_size"
	logNLibriPutters       = "libri_n_putters"
	logClientIDFilepath    = "client_id_filepath"
	logGCPProjectID        = "gcp_project_id"
	logStorage             = "storage"
	logLibrarians          = "librarians"
	logCatalog             = "catalog"
	logCatalogPutTimeout   = "catalog_put_timeout"
	logCatalogPutQueueSize = "catalog_put_queue_size"
	logNCatalogPutters     = "catalog_n_putters"
	logAuthorPubShort      = "author_pub_short"
	logReaderPubShort      = "reader_pub_short"
	logAuthorEntityID      = "author_entity_id"
	logReaderEntityID      = "reader_entity_id"
	logEnvelopeKey         = "envelope_key"
)

func logGetEntityIDs(rq *keyapi.GetPublicKeyDetailsRequest) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logAuthorPubShort, shortHex(rq.PublicKeys[0])),
		zap.String(logReaderPubShort, shortHex(rq.PublicKeys[1])),
	}
}

func logGotEntityIDs(
	rq *keyapi.GetPublicKeyDetailsRequest, rp *keyapi.GetPublicKeyDetailsResponse,
) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logAuthorPubShort, shortHex(rq.PublicKeys[0])),
		zap.String(logAuthorEntityID, rp.PublicKeyDetails[0].EntityId),
		zap.String(logReaderPubShort, shortHex(rq.PublicKeys[1])),
		zap.String(logReaderEntityID, rp.PublicKeyDetails[1].EntityId),
	}
}

func logPutPublication(rq *catalogapi.PutRequest) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logEnvelopeKey, hex.EncodeToString(rq.Value.EnvelopeKey)),
	}
}

func shortHex(val []byte) string {
	return hex.EncodeToString(val[:8])
}
