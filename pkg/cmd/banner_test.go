package cmd

import (
	"os"
	"testing"
)

func TestWriteBanner(t *testing.T) {
	// just test that there are no panics from mismatched template vars
	writeBanner(os.Stdout)
}
