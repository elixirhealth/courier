package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

var courierCmd = &cobra.Command{
	Use:   "courier",
	Short: "communication between the libri cluster and the rest of the elxir stack",
}

func Execute() {
	if err := courierCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
