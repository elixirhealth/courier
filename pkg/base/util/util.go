package util

import "math/rand"

// RandBytes generates a random bytes slice of a given length.
func RandBytes(rng *rand.Rand, length int) []byte {
	b := make([]byte, length)
	_, err := rng.Read(b)
	MaybePanic(err)
	return b
}

// MaybePanic panics if the error is not nil.
func MaybePanic(err error) {
	if err != nil {
		panic(err)
	}
}
