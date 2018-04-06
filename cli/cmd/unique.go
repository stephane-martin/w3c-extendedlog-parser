package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

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
		uniqueHashes := make(map[string]map[string]bool)
		// date => number
		totals := make(map[string]uint64)
		for _, file := range inputFiles {
			err = uniqueFile(file, &uniqueHashes, &totals)
			fatal(err)
			fmt.Fprintf(os.Stderr, "%d unique lines / %d\n", countHashes(uniqueHashes), countTotal(totals))
		}
		fmt.Fprintln(os.Stderr)
		for date := range uniqueHashes {
			fmt.Fprintf(
				os.Stderr,
				"%s: %d unique lines / %d (%d% duplicates)\n",
				date, len(uniqueHashes[date]), totals[date], 100-int(float64(100*len(uniqueHashes[date]))/float64(totals[date])),
			)
		}

	},
}

func countHashes(allhashes map[string]map[string]bool) (total uint64) {
	for _, hashes := range allhashes {
		total += uint64(len(hashes))
	}
	return total
}

func countTotal(totals map[string]uint64) (total uint64) {
	for _, nb := range totals {
		total += nb
	}
	return total
}

func uniqueFile(fname string, uniques *map[string]map[string]bool, totals *map[string]uint64) error {
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
	var h string
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
		h = string(murmur3.New128().Sum(lineB))
		if (*uniques)[date] == nil {
			(*uniques)[date] = make(map[string]bool)
		}
		(*uniques)[date][h] = true
	}
	return nil

}

func init() {
	rootCmd.AddCommand(uniqueCmd)
	uniqueCmd.Flags().StringVar(&input, "input", "", "input directory")
	uniqueCmd.Flags().StringVar(&extension, "ext", "log", "only select input files with that extension")
}
