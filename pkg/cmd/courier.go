package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

var courierCmd = &cobra.Command{
	Use:   "courier",
	Short: "courier communicates between libri cluster and the rest of the elxir stack",
}

// Execute runs the root courier command.
func Execute() {
	if err := courierCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
