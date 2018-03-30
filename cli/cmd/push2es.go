package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/inconshreveable/log15"
	"github.com/olivere/elastic"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

var push2esCmd = &cobra.Command{
	Use:   "push2es",
	Short: "Parse accesslog files and push events to Elasticsearch",
	Run: func(cmd *cobra.Command, args []string) {
		if len(fnames) == 0 {
			fatal(errors.New("specify the files to be parsed"))
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
		fatal(err)

		version, err := client.ElasticsearchVersion(esURL)
		fatal(err)
		fmt.Fprintln(os.Stdout, "Elasticsearch version:", version)

		ctx := context.Background()

		proc, err := client.BulkProcessor().
			Name("push2esWorker").
			Workers(http.DefaultMaxIdleConnsPerHost).
			BulkActions(-1).
			BulkSize(-1).
			Backoff(elastic.StopBackoff{}).
			Do(ctx)

		fatal(err)

		lines := make([]*parser.Line, 0, 1000)
		var l *parser.Line

		for _, fname := range fnames {
			lines = lines[:0]
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
			fieldNames = p.FieldNames()

			linePool := &sync.Pool{
				New: func() interface{} {
					return parser.NewLine(fieldNames)
				},
			}

			for {
				l, err = p.NextTo(linePool.Get().(*parser.Line))
				if l == nil || err != nil {
					break
				}
				lines = append(lines, l)
				proc.Add(elastic.NewBulkIndexRequest().Doc(l).Index(indexName).Type("accesslogs"))
				if len(lines) == 1000 {
					fatal(proc.Flush())
					for _, l = range lines {
						linePool.Put(l)
					}
					lines = lines[:0]
				}
			}
			if len(lines) > 0 {
				fatal(proc.Flush())
				for _, l = range lines {
					linePool.Put(l)
				}
				lines = lines[:0]
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
