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
var suffix bool

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
	ret = strings.Replace(ret, "__", "_", -1)
	ret = strings.Trim(ret, "_")
	return ret
}

func suffixHeaders(header string) (ret string) {
	switch parser.GuessType(header) {
	case parser.MyDate:
		return header + "_date"
	case parser.MyIP:
		return header + "_ip"
	case parser.MyTime:
		return header + "_time"
	case parser.MyTimestamp:
		return header + "_timestamp"
	case parser.MyURI:
		return header + "_uri"
	case parser.Float64:
		return header + "_float"
	case parser.Int64:
		return header + "_int"
	case parser.Bool:
		return header + "_bool"
	case parser.String:
		return header + "_str"
	}
	return header + "_str"
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
		if !jsonExport && !csvExport {
			jsonExport = true
		}

		for _, fname := range fnames {
			fname = strings.TrimSpace(fname)
			f, err := os.Open(fname)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening '%s': %s\n", fname, err)
				continue
			}
			err = doParse(f, os.Stdout, jsonExport, csvExport, suffix)
			f.Close()
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error building parser:", err)
			}

		}
	},
}

func doParse(in io.Reader, out io.Writer, doJSON bool, doCSV bool, printSuffix bool) error {
	p := parser.NewFileParser(in)
	err := p.ParseHeader()
	if err != nil {
		return err
	}
	fieldNames = p.FieldNames()
	if doCSV {
		// print header line
		if printSuffix {
			fmt.Fprintln(out, strings.Join(
				foreach(
					foreach(fieldNames, suffixHeaders),
					sanitize,
				),
				",",
			))
		} else {
			fmt.Fprintln(out, strings.Join(foreach(fieldNames, sanitize), ","))
		}
	}
	var l *parser.Line
	for {
		l, err = p.NextTo(l)
		if l == nil || err != nil {
			break
		}
		err = l.WriteTo(out, doJSON)
		if err != nil {
			return err
		}
	}
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func init() {
	rootCmd.AddCommand(parseCmd)
	parseCmd.Flags().StringArrayVar(&fnames, "filename", []string{}, "the files to parse")
	parseCmd.Flags().BoolVar(&jsonExport, "json", false, "print the logs as JSON")
	parseCmd.Flags().BoolVar(&csvExport, "csv", false, "print the logs as CSV")
	parseCmd.Flags().BoolVar(&suffix, "suffix", false, "when exporting to CSV, suffix the field names with data type")
}
