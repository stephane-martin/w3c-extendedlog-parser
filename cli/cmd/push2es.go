package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
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
		if len(filenames) == 0 {
			fatal(errors.New("specify the files to be parsed"))
		}

		logger := log15.New()
		logger.SetHandler(log15.StderrHandler)

		client, err := getESClient(esURL, username, password, logger)
		fatal(err)
		for report := range uploadFilesES(client, filenames, batchsize) {
			if report.err != nil {
				fmt.Fprintf(os.Stderr, "Failed to upload '%s': %s\n", report.filename, report.err.Error())
			} else {
				fmt.Fprintf(os.Stderr, "Uploaded '%s': %d lines\n", report.filename, report.nbLines)
			}
		}
	},
}

type processor struct {
	*elastic.BulkProcessor
	lines      []*parser.Line
	pool       *sync.Pool
	fieldNames []string
}

func newProcessor(client *elastic.Client, fieldNames []string, size int) (*processor, error) {
	proc, err := client.BulkProcessor().
		Name("push2esWorker").
		Workers(http.DefaultMaxIdleConnsPerHost).
		BulkActions(-1).
		BulkSize(-1).
		Backoff(elastic.StopBackoff{}).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	p := &processor{
		BulkProcessor: proc,
		lines:         make([]*parser.Line, 0, size),
		fieldNames:    fieldNames,
	}
	p.pool = &sync.Pool{
		New: func() interface{} {
			return parser.NewLine(p.fieldNames)
		},
	}
	return p, nil
}

func (p *processor) flush() (nbLines int, err error) {
	nbLines = len(p.lines)
	if nbLines == 0 {
		return 0, nil
	}
	err = p.Flush()
	if err != nil {
		return 0, err
	}
	for _, l := range p.lines {
		p.pool.Put(l)
	}
	p.lines = p.lines[:0]
	return nbLines, nil
}

func (p *processor) add(line *parser.Line) {
	p.lines = append(p.lines, line)
	p.Add(elastic.NewBulkIndexRequest().Doc(line).Index(indexName).Type("accesslogs"))
}

func (p *processor) getLine() *parser.Line {
	return p.pool.Get().(*parser.Line)
}

func (p *processor) len() int {
	return len(p.lines)
}

func uploadES(f io.Reader, client *elastic.Client, size int) (nbLines int, err error) {

	p := parser.NewFileParser(f)
	err = p.ParseHeader()
	if err != nil {
		return 0, err
	}
	fieldNames := p.FieldNames()

	proc, err := newProcessor(client, fieldNames, size)
	if err != nil {
		return 0, err
	}

	var l *parser.Line

	for {
		l, err = p.NextTo(proc.getLine())
		if l == nil || err != nil {
			break
		}
		proc.add(l)
		if proc.len() >= size {
			nb, err := proc.flush()
			if err != nil {
				return 0, err
			}
			nbLines = nbLines + nb
		}
	}
	if proc.len() > 0 {
		nb, err := proc.flush()
		if err != nil {
			return 0, err
		}
		nbLines = nbLines + nb
	}
	return nbLines, nil

}

func uploadFileES(client *elastic.Client, fname string, size int) (nbLines int, err error) {
	fname = strings.TrimSpace(fname)
	f, err := os.Open(fname)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	nbLines, err = uploadES(f, client, size)
	if err != nil {
		return 0, err
	}
	return nbLines, nil
}

type uploadReport struct {
	filename string
	err      error
	nbLines  int
}

func uploadFilesES(client *elastic.Client, fnames []string, size int) chan uploadReport {
	c := make(chan uploadReport)
	go func() {
		for _, fname := range fnames {
			nbLines, err := uploadFileES(client, fname, size)
			c <- uploadReport{filename: fname, err: err, nbLines: nbLines}
		}
		close(c)
	}()
	return c
}

func getESClient(url string, username string, password string, logger log15.Logger) (*elastic.Client, error) {
	elasticOpts := []elastic.ClientOptionFunc{}
	elasticOpts = append(elasticOpts, elastic.SetURL(url))
	elasticOpts = append(elasticOpts, elastic.SetErrorLog(&esLogger{Logger: logger}))

	if strings.HasPrefix(url, "https://") {
		elasticOpts = append(elasticOpts, elastic.SetScheme("https"))
	}
	if len(username) > 0 && len(password) > 0 {
		elasticOpts = append(elasticOpts, elastic.SetBasicAuth(username, password))
	}

	client, err := elastic.NewClient(elasticOpts...)
	if err != nil {
		return nil, err
	}
	version, err := client.ElasticsearchVersion(esURL)
	if err != nil {
		return nil, err
	}
	logger.Info("Elasticsearch version", "version", version)
	return client, nil
}

func init() {
	rootCmd.AddCommand(push2esCmd)
	push2esCmd.Flags().StringArrayVar(&filenames, "filename", []string{}, "The files to parse")
	push2esCmd.Flags().StringVar(&esURL, "url", "http://127.0.0.1:9200", "ES connection url")
	push2esCmd.Flags().StringVar(&indexName, "index", "accesslogs", "Name of index to create")
	push2esCmd.Flags().StringVar(&username, "username", "", "Username for http basic auth")
	push2esCmd.Flags().StringVar(&password, "password", "", "Password for http basic auth")
	push2esCmd.Flags().IntVar(&batchsize, "batchsize", 5000, "Batch size to upload to ES")
}
