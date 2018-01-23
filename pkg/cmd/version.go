package cmd

import (
	"os"

	"github.com/elxirhealth/courier/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "print the courier version",
	Long:  "print the courier version",
	RunE: func(cmd *cobra.Command, args []string) error {
		_, err := os.Stdout.WriteString(version.Current.Version.String() + "\n")
		return err
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
