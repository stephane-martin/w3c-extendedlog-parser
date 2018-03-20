package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

var fnames = make([]string, 0)
var jsonExport bool
var csvExport bool

func foreach(sl []string, f func(string) string) (ret []string) {
	ret = make([]string, 0, len(sl))
	for _, s := range sl {
		ret = append(ret, f(s))
	}
	return ret
}

func sanitize(header string) (ret string) {
	ret = strings.Replace(header, "(", "_", -1)
	ret = strings.Replace(ret, ")", "_", -1)
	ret = strings.Replace(ret, "-", "_", -1)
	return ret
}

var parseCmd = &cobra.Command{
	Use:   "parse",
	Short: "Parse an access log file and print the lines it as JSON or CSV",
	Run: func(cmd *cobra.Command, args []string) {
		if len(fnames) == 0 {
			fatal(errors.New("specify the files to be parsed"))
		}
		if jsonExport && csvExport {
			fatal(errors.New("--json and --csv are exclusive"))
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
			if !jsonExport && !csvExport {
				jsonExport = true
			}
			if csvExport {
				// print header line
				fmt.Println(strings.Join(foreach(p.FieldNames, sanitize), ","))
			}
			var l *parser.Line
			for {
				l, err = p.Next()
				if l == nil || err != nil {
					break
				}
				err = l.WriteTo(os.Stdout, jsonExport)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
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
	parseCmd.Flags().BoolVar(&jsonExport, "json", false, "print the logs as JSON")
	parseCmd.Flags().BoolVar(&csvExport, "csv", false, "print the logs as CSV")
}
