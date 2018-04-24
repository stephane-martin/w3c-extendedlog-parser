package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

var pushdir2esCmd = &cobra.Command{
	Use:   "pushdir2es",
	Short: "Parse all files in some directory and push events to elasticsearch",
	Run: func(cmd *cobra.Command, args []string) {
		if batchsize <= 0 {
			batchsize = 5000
		}
		if len(input) == 0 {
			fatal(errors.New("specify an input directory"))
		}
		curdir, err := os.Getwd()
		fatal(err)
		curdir, err = filepath.Abs(curdir)
		fatal(err)
		input, err = filepath.Abs(input)
		fatal(err)

		inputFiles, err := findFiles(input, extension)
		fatal(err)

		if len(inputFiles) == 0 {
			fmt.Fprintln(os.Stderr, "No file to process.")
			return
		}
		fmt.Fprintln(os.Stderr, "Will process the following files")
		for _, fname := range inputFiles {
			fmt.Fprintf(os.Stderr, "- %s\n", fname)
		}
		fmt.Fprintln(os.Stderr)

		logger := log15.New()
		logger.SetHandler(log15.StderrHandler)

		client, err := getESClient(esURL, username, password, logger)
		fatal(err)
		for report := range uploadFilesES(client, inputFiles, batchsize) {
			if report.err != nil {
				fmt.Fprintf(os.Stderr, "Failed to upload '%s': %s\n", report.filename, report.err.Error())
			} else {
				fmt.Fprintf(os.Stderr, "Uploaded '%s': %d lines\n", report.filename, report.nbLines)
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(pushdir2esCmd)
	pushdir2esCmd.Flags().StringVar(&input, "input", "", "input directory")
	pushdir2esCmd.Flags().StringVar(&extension, "ext", "log", "only select input files with that extension")
	pushdir2esCmd.Flags().StringVar(&esURL, "url", "http://127.0.0.1:9200", "Elasticsearch connection URL")
	pushdir2esCmd.Flags().StringVar(&indexName, "index", "accesslogs", "Name of ES index to use")
	pushdir2esCmd.Flags().StringVar(&username, "username", "", "username for HTTP Basic Auth")
	pushdir2esCmd.Flags().StringVar(&password, "password", "", "password for HTTP Basic Auth")
	pushdir2esCmd.Flags().IntVar(&batchsize, "batchsize", 5000, "batch size to upload to ES")
}
