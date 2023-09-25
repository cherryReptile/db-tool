package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dbtool",
	Short: "Tool for db(some useful stuff that was pretty boring to deal with)",
	Long:  "Tool for db(some useful stuff that was pretty boring to deal with)... yea, this is long description",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
