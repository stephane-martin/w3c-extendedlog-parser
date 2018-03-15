package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

var fnames = make([]string, 0)

// parseCmd represents the parse command
var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Parse an access log file and prints it as JSON lines",
	Run: func(cmd *cobra.Command, args []string) {
		if len(fnames) == 0 {
			fmt.Fprintln(os.Stderr, "specify the files to be parsed")
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
			for {
				l, err = p.Next()
				if l == nil || err != nil {
					break
				}
				b, err := l.MarshalJSON()
				if err == nil {
					fmt.Println(string(b))
				}
			}
			if err != nil && err != io.EOF {
				fmt.Fprintln(os.Stderr, err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(parseCmd)
	parseCmd.Flags().StringArrayVar(&fnames, "filename", []string{}, "the files to parse")
}
