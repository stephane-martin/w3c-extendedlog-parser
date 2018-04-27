package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

// dropIndexCmd represents the dropIndex command
var dropIndexCmd = &cobra.Command{
	Use:   "drop-index",
	Short: "Drop Elasticsearch index",
	Run: func(cmd *cobra.Command, args []string) {
		logger := log15.New()
		logger.SetHandler(log15.StderrHandler)
		params := esParams{url: esURL, username: username, password: password}

		client, err := getESClient(params, logger)
		fatal(err)
		resp, err := client.DeleteIndex(indexName).Do(context.Background())
		fatal(err)
		if !resp.Acknowledged {
			fmt.Fprintln(os.Stderr, "Not acknowledged by ES!")
		}
		fmt.Fprintln(os.Stderr, "Dropped:", indexName)
	},
}

func init() {
	rootCmd.AddCommand(dropIndexCmd)
	dropIndexCmd.Flags().StringVar(&esURL, "url", "http://127.0.0.1:9200", "ES connection url")
	dropIndexCmd.Flags().StringVar(&indexName, "index", "accesslogs", "Name of ES index to delete")
	dropIndexCmd.Flags().StringVar(&username, "username", "", "Username for http basic auth")
	dropIndexCmd.Flags().StringVar(&password, "password", "", "Password for http basic auth")
}
