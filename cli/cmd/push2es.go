package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/inconshreveable/log15"
	"github.com/olivere/elastic"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

// push2esCmd represents the push2es command
var push2esCmd = &cobra.Command{
	Use:   "push2es",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(fnames) == 0 {
			fmt.Fprintln(os.Stderr, "specify the files to be parsed")
			os.Exit(-1)
		}

		logger := log15.New()
		logger.SetHandler(log15.StderrHandler)

		elasticOpts := []elastic.ClientOptionFunc{}
		elasticOpts = append(elasticOpts, elastic.SetURL(esURL))
		elasticOpts = append(elasticOpts, elastic.SetErrorLog(&esLogger{Logger: logger}))

		if strings.HasPrefix(esURL, "https://") {
			elasticOpts = append(elasticOpts, elastic.SetScheme("https"))
		}
		if len(username) > 0 && len(password) > 0 {
			elasticOpts = append(elasticOpts, elastic.SetBasicAuth(username, password))
		}

		client, err := elastic.NewClient(elasticOpts...)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-1)
		}

		version, err := client.ElasticsearchVersion(esURL)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-1)
		}
		fmt.Fprintln(os.Stdout, "Elasticsearch version:", version)

		ctx := context.Background()

		proc, err := client.BulkProcessor().
			Name("push2esWorker").
			Workers(http.DefaultMaxIdleConnsPerHost).
			BulkActions(-1).
			BulkSize(-1).
			Backoff(elastic.StopBackoff{}).
			Do(ctx)

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-1)
		}

		for _, fname := range fnames {
			fname = strings.TrimSpace(fname)
			f, err := os.Open(fname)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening '%s': %s\n", fname, err)
				continue
			}
			defer f.Close()

			p := parser.NewFileParser(f)
			err = p.ParseHeader()
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error building parser:", err)
				continue
			}
			var l *parser.Line
			var i int
			for {
				l, err = p.Next()
				if l == nil || err != nil {
					break
				}
				i++
				proc.Add(elastic.NewBulkIndexRequest().Doc(l).Index(indexName).Type("accesslogs"))
				if i >= 1000 {
					err = proc.Flush()
					if err != nil {
						fmt.Fprintln(os.Stderr, err)
						os.Exit(-1)
					}
					i = 0
				}
			}
			if i > 0 {
				err = proc.Flush()
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(-1)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(push2esCmd)
	push2esCmd.Flags().StringArrayVar(&fnames, "filename", []string{}, "the files to parse")
	push2esCmd.Flags().StringVar(&esURL, "url", "http://127.0.0.1:9200", "Elasticsearch connection URL")
	push2esCmd.Flags().StringVar(&indexName, "index", "accesslogs", "Name of index to create")
	push2esCmd.Flags().StringVar(&username, "username", "", "username for HTTP Basic Auth")
	push2esCmd.Flags().StringVar(&password, "password", "", "password for HTTP Basic Auth")

}
