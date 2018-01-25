package cmd

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetHealthChecker(t *testing.T) {
	couriers := "localhost:1234 localhost:5678"
	viper.Set(couriersFlag, couriers)
	hc, err := getHealthChecker()
	assert.Nil(t, err)
	assert.NotNil(t, hc)

	couriers = "1234"
	viper.Set(couriersFlag, couriers)
	hc, err = getHealthChecker()
	assert.NotNil(t, err)
	assert.Nil(t, hc)
}
