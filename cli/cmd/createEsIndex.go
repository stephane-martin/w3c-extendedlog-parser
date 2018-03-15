package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/olivere/elastic"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

var esURL string
var indexName string
var username string
var password string

var createEsIndexCmd = &cobra.Command{
	Use:   "create-es-index",
	Short: "Create an index in Elasticsearch with adequate mapping",
	Run: func(cmd *cobra.Command, args []string) {
		fieldsLine = strings.TrimSpace(fieldsLine)
		fname = strings.TrimSpace(fname)
		if len(fieldsLine) == 0 && len(fname) == 0 {
			fmt.Fprintln(os.Stderr, "Please specify fields")
			os.Exit(-1)
		}
		if len(fieldsLine) != 0 && len(fname) != 0 {
			fmt.Fprintln(os.Stderr, "--fields and --filename options are exclusive")
			os.Exit(-1)
		}
		if len(fieldsLine) > 0 {
			fieldNames = strings.Split(fieldsLine, " ")
		} else {
			f, err := os.Open(fname)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(-1)
			}
			p := parser.NewFileParser(f)
			err = p.ParseHeader()
			f.Close()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(-1)
			}
			fieldNames = p.FieldNames
		}
		if len(fieldNames) == 0 {
			fmt.Fprintln(os.Stderr, "field names not found")
			os.Exit(-1)
		}

		opts := newEsOpts(shards, replicas, check, time.Duration(refreshInterval)*time.Second, fieldNames)
		optionsBody, err := json.Marshal(opts)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
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
		exists, err := client.IndexExists(indexName).Do(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-1)
		}

		if exists {
			fmt.Fprintf(os.Stderr, "Index '%s' already exists\n", indexName)
			os.Exit(0)
		}

		createIndex, err := client.CreateIndex(indexName).BodyString(string(optionsBody)).Do(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(-1)
		}
		if !createIndex.Acknowledged {
			fmt.Fprintln(os.Stderr, "Not acknowledged")
			os.Exit(-1)
		}
		fmt.Fprintf(os.Stderr, "Index created: '%s'\n", indexName)
	},
}

func init() {
	rootCmd.AddCommand(createEsIndexCmd)
	createEsIndexCmd.Flags().StringVar(&fieldsLine, "fields", "", "specify the fields that will be present in the access logs")
	createEsIndexCmd.Flags().StringVar(&fname, "filename", "", "specify the log file from which to extract the fields")
	createEsIndexCmd.Flags().UintVar(&shards, "shards", 1, "number of shards for the index")
	createEsIndexCmd.Flags().UintVar(&replicas, "replicas", 0, "number of replicas for the index")
	createEsIndexCmd.Flags().BoolVar(&check, "check", false, "whether to check the index on startup")
	createEsIndexCmd.Flags().IntVar(&refreshInterval, "refresh", 1, "refresh interval in seconds")
	createEsIndexCmd.Flags().StringVar(&esURL, "url", "http://127.0.0.1:9200", "Elasticsearch connection URL")
	createEsIndexCmd.Flags().StringVar(&indexName, "index", "accesslogs", "Name of index to create")
	createEsIndexCmd.Flags().StringVar(&username, "username", "", "username for HTTP Basic Auth")
	createEsIndexCmd.Flags().StringVar(&password, "password", "", "password for HTTP Basic Auth")
}

type ESLogger struct {
	Logger log15.Logger
}

func (l *ESLogger) Printf(format string, v ...interface{}) {
	l.Logger.Info(fmt.Sprintf(format, v...))
}
