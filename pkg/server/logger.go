package server

import (
	"github.com/drausin/libri/libri/common/id"
	"github.com/elxirhealth/courier/pkg/courierapi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logDocKey    = "document_key"
	logKey       = "key"
	logOperation = "operation"
)

func putDocumentFields(rq *courierapi.PutRequest, rp *courierapi.PutResponse) []zapcore.Field {
	return []zapcore.Field{
		zap.String(logKey, id.Hex(rq.Key)),
		zap.String(logOperation, courierapi.PutOperation_name[int32(rp.Operation)]),
	}
}
