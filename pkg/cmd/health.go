package cmd

import (
	"log"
	"os"

	lserver "github.com/drausin/libri/libri/common/logging"
	server2 "github.com/drausin/libri/libri/librarian/server"
	"github.com/elxirhealth/service-base/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "test health of one or more courier servers",
	RunE: func(cmd *cobra.Command, args []string) error {
		hc, err := getHealthChecker()
		if err != nil {
			log.Println(err.Error())
			return err
		}
		if allOk, _ := hc.Check(); !allOk {
			os.Exit(1)
		}
		return nil
	},
}

func init() {
	testCmd.AddCommand(healthCmd)
}

func getHealthChecker() (server.HealthChecker, error) {
	addrs, err := server2.ParseAddrs(viper.GetStringSlice(couriersFlag))
	if err != nil {
		return nil, err
	}
	lg := lserver.NewDevLogger(lserver.GetLogLevel(viper.GetString(logLevelFlag)))
	return server.NewHealthChecker(server.NewInsecureDialer(), addrs, lg)
}
