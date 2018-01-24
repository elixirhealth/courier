package cmd

import (
	"errors"

	cerrors "github.com/drausin/libri/libri/common/errors"
	lserver "github.com/drausin/libri/libri/common/logging"
	server2 "github.com/drausin/libri/libri/librarian/server"
	"github.com/elxirhealth/service-base/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	couriersFlag = "couriers"
	timeoutFlag  = "timeout"
)

var errFailedHealthcheck = errors.New("some or all couriers unhealthy")

// testCmd represents the test command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "test one or more courier servers",
}

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "test health of one or more courier servers",
	RunE: func(cmd *cobra.Command, args []string) error {
		hc, err := getHealthChecker()
		if err != nil {
			return nil
		}
		if allOk, _ := hc.Check(); !allOk {
			return errFailedHealthcheck
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(testCmd)

	testCmd.PersistentFlags().StringSlice(couriersFlag, nil,
		"space-separated addresses of courier(s)")

	testCmd.AddCommand(healthCmd)

	// bind viper flags
	viper.SetEnvPrefix(envVarPrefix) // look for env vars with "LIBRI_" prefix
	viper.AutomaticEnv()             // read in environment variables that match
	cerrors.MaybePanic(viper.BindPFlags(testCmd.PersistentFlags()))
}

func getHealthChecker() (server.HealthChecker, error) {
	addrs, err := server2.ParseAddrs(viper.GetStringSlice(librariansFlag))
	if err != nil {
		return nil, err
	}
	lg := lserver.NewDevLogger(lserver.GetLogLevel(viper.GetString(logLevelFlag)))
	return server.NewHealthChecker(server.NewInsecureDialer(), addrs, lg)
}
