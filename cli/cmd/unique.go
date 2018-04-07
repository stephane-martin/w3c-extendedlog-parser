package cmd

import (
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"sort"

	"github.com/clarkduvall/hyperloglog"
	"github.com/spaolacci/murmur3"
	"github.com/spf13/cobra"
	parser "github.com/stephane-martin/w3c-extendedlog-parser"
)

// uniqueCmd represents the unique command
var uniqueCmd = &cobra.Command{
	Use:   "unique",
	Short: "Count the number of unique lines",
	Run: func(cmd *cobra.Command, args []string) {
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
		// date => hash => bool
		uniques := make(map[string]*hyperloglog.HyperLogLogPlus)
		// date => number
		totals := make(map[string]uint64)
		for _, file := range inputFiles {
			err = uniqueFile(file, &uniques, &totals)
			fatal(err)
			fmt.Fprintf(os.Stderr, "%d unique lines / %d\n", count(uniques), countTotal(totals))
		}

		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Summary: %d unique lines / %d\n", count(uniques), countTotal(totals))
		fmt.Fprintln(os.Stderr)

		alldates := make([]string, 0, len(uniques))
		for date := range uniques {
			alldates = append(alldates, date)
		}
		sort.Strings(alldates)

		for _, date := range alldates {
			fmt.Fprintf(
				os.Stderr,
				"%s: %d unique lines / %d (%d%% duplicates)\n",
				date, uniques[date].Count(), totals[date], 100-int(float64(100*uniques[date].Count())/float64(totals[date])),
			)
		}

	},
}

func count(allhashes map[string]*hyperloglog.HyperLogLogPlus) (total uint64) {
	for _, hashes := range allhashes {
		total += uint64(hashes.Count())
	}
	return total
}

func countTotal(totals map[string]uint64) (total uint64) {
	for _, nb := range totals {
		total += nb
	}
	return total
}

func uniqueFile(fname string, uniques *map[string]*hyperloglog.HyperLogLogPlus, totals *map[string]uint64) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	p := parser.NewFileParser(f)
	err = p.ParseHeader()
	if err != nil {
		return err
	}
	var line *parser.Line
	var h hash.Hash64
	var date string
	var lineB []byte

	for {
		line, err = p.NextTo(line)
		if line == nil || err != nil {
			break
		}
		date = line.GetDate().String()
		(*totals)[date]++
		lineB, err = line.MarshalJSON()
		h = murmur3.New64()
		h.Write(lineB)
		if (*uniques)[date] == nil {
			(*uniques)[date], _ = hyperloglog.NewPlus(18)
		}
		(*uniques)[date].Add(h)
	}
	return nil

}

func init() {
	rootCmd.AddCommand(uniqueCmd)
	uniqueCmd.Flags().StringVar(&input, "input", "", "input directory")
	uniqueCmd.Flags().StringVar(&extension, "ext", "log", "only select input files with that extension")
}
