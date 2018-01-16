package util

import (
	"math/rand"

	"github.com/drausin/libri/libri/common/errors"
)

// RandBytes generates a random bytes slice of a given length.
func RandBytes(rng *rand.Rand, length int) []byte {
	b := make([]byte, length)
	_, err := rng.Read(b)
	errors.MaybePanic(err)
	return b
}
