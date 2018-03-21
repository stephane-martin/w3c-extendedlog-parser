package cmd

import (
	"github.com/spf13/cobra"
)

var parseDirCmd = &cobra.Command{
	Use:   "parse-dir",
	Short: "Parse every file of some input directory",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	rootCmd.AddCommand(parseDirCmd)
}
