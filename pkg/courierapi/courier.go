package courierapi

import (
	"bytes"
	"crypto/sha256"

	"github.com/drausin/libri/libri/common/id"
	"github.com/drausin/libri/libri/librarian/api"
	"github.com/elxirhealth/courier/pkg/base/util"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	// ErrKeyNotValueHash indicates when the key is not the SHA-256 hash of the value bytes.
	ErrKeyNotValueHash = errors.New("key is not hash of value")
)

// ValidateGetRequest checks that the fields of a GetRequest are populated correctly.
func ValidateGetRequest(rq *GetRequest) error {
	return api.ValidateBytes(rq.Key, id.Length, "request key")
}

// ValidatePutRequest checks that the fields of a PutRequest are populated correctly.
func ValidatePutRequest(rq *PutRequest) error {
	if err := api.ValidateBytes(rq.Key, id.Length, "request key"); err != nil {
		return err
	}
	if err := api.ValidateDocument(rq.Value); err != nil {
		return err
	}
	valueBytes, err := proto.Marshal(rq.Value)
	util.MaybePanic(err) // should never happen b/c just unmarshaled from wire
	hash := sha256.Sum256(valueBytes)
	if !bytes.Equal(rq.Key, hash[:]) {
		return ErrKeyNotValueHash
	}
	return nil
}
