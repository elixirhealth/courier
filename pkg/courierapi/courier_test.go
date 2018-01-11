package courierapi

import (
	"math/rand"
	"testing"

	"github.com/drausin/libri/libri/librarian/api"
	"github.com/elxirhealth/courier/pkg/util"
	"github.com/stretchr/testify/assert"
)

func TestValidateGetRequest(t *testing.T) {
	rng := rand.New(rand.NewSource(0))

	// ok
	rq := &GetRequest{Key: util.RandBytes(rng, 32)}
	err := ValidateGetRequest(rq)
	assert.Nil(t, err)

	// missing key
	rq = &GetRequest{}
	err = ValidateGetRequest(rq)
	assert.NotNil(t, err)
}

func TestValidatePutRequest(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	value, key := api.NewTestDocument(rng)

	okRq := &PutRequest{
		Key:   key.Bytes(),
		Value: value,
	}
	err := ValidatePutRequest(okRq)
	assert.Nil(t, err)

	badRqs := map[string]*PutRequest{
		"missing key": {
			Value: value,
		},
		"missing value": {
			Key: key.Bytes(),
		},
		"key not hash of value": {
			Key:   util.RandBytes(rng, 32),
			Value: value,
		},
	}

	for desc, badRq := range badRqs {
		err = ValidatePutRequest(badRq)
		assert.NotNil(t, err, desc)
	}
}
