package courierapi

import (
	"github.com/drausin/libri/libri/common/id"
	"github.com/drausin/libri/libri/librarian/api"
)

func ValidateGetRequest(rq *GetRequest) error {
	return api.ValidateBytes(rq.Key, id.Length, "request key")
}

func ValidatePutRequest(rq *PutRequest) error {
	if err := api.ValidateBytes(rq.Key, id.Length, "request key"); err != nil {
		return err
	}
	if err := api.ValidateDocument(rq.Value); err != nil {
		return err
	}
	return nil
}
